import time
import datetime
from dateutil.parser import parse as datetime_parse
from tzlocal import get_localzone
import pytz

import os
import simplejson as json

import psycopg2
from psycopg2.extensions import AsIs
import argparse

import shapely
from shapely.geometry import mapping, shape, Point

from .pgpointcloud import (
    DATA_TYPE_MAPPING,
    build_pc_dimension, build_pc_schema, add_pc_schema,
    create_pcpatch_table, create_temp_table,
    insert_pcpoints, copy_pcpoints, insert_pcpatches, make_wkb_point
)

COORDINATES = ['X', 'Y', 'Z']

OVERRIDE_INPUT_FORMAT = [
    ['date', datetime_parse],
    ['time', datetime_parse],
    ['datetime', datetime_parse],
]

Config = {
    'input_file': None,
    'dsn': None,
    'metadata': None,
    'group_by': [],
    'ignore': [],
    'srid': None,
    'pcid': None,
    'table_name': None,
    'table_action': None,
    'date': [],
    'time': [],
    'datetime': [],
    'timezone': get_localzone(),
    'copy_mode': False,
    'buffer_size': 1000
}
DSIn = None
DBConn = None

def open_input_file(f):

    global DSIn

    DSIn = json.load(open(f, 'r'))

    if DSIn is None or DSIn.get('type', None) != 'FeatureCollection':
        raise

    return DSIn

def open_db_connection(dsn):
    return psycopg2.connect(dsn)

def interpret_fields(layer):

    def add_coordinate(dimensions, coord):
        field_type = float
        dimensions.append({
            'index': None,
            'name': coord,
            'type': {
                'source': field_type,
                'dest': DATA_TYPE_MAPPING[field_type]
            }
        })

    fields = {
        'group_by': [],
        'ignore': [],
        'dimension': [],
        'overrides': {}
    }

    if len(layer) < 1:
        raise

    add_coordinate(fields['dimension'], 'X')
    add_coordinate(fields['dimension'], 'Y')
    add_coordinate(fields['dimension'], 'Z')

    # use the first feature
    feat = layer[0]
    properties = feat['properties']
    keys = properties.keys()
    keys.sort()

    group_by = Config.get('group_by', [])
    ignore = Config.get('ignore', [])

    # date, time, datetime overrides
    overrides = {}
    for override_field, override_callback in OVERRIDE_INPUT_FORMAT:
        column = Config.get(override_field, [])

        overrides[override_field] = {
            'column': column,
            'callback': override_callback
        }

    # loop over each field
    for idx in xrange(len(keys)):
        key = keys[idx]

        field_info = {
            'index': idx,
            'name': key, 
        }

        field_type = type(properties[key])

        # field in ignore list
        if field_info['name'] in ignore:
            fields['ignore'].append(field_info)
            continue

        # user-defined group_by list and field in that list 
        if group_by and field_info['name'] in group_by:
            # add field to internal group_by list
            fields['group_by'].append(field_info)
        # field is string format
        elif field_type == str:
            # field not in user-defined group_by list
            if not group_by:

                # field in override
                found = False
                for treat_as, details in overrides.iteritems():
                    if field_info['name'] in details['column']:
                        found = True
                        break

                if found:

                    if treat_as == 'date':
                        source = datetime.date 
                    elif treat_as == 'time':
                        source = datetime.time
                    elif treat_as == 'datetime':
                        source = datetime.datetime

                    dest = DATA_TYPE_MAPPING[source]

                    field_info['type'] = {
                        'treat_as': treat_as,
                        'callback': details['callback'],
                        'source': source,
                        'dest': dest,
                    }
                    fields['dimension'].append(field_info)

                # default is to add to group_by
                else:
                    fields['group_by'].append(field_info)

            # add field to internal ignore list
            else:
                fields['ignore'].append(field_info)
        # field is supported
        elif field_type in DATA_TYPE_MAPPING:
            field_info['type'] = {
                'source': field_type,
                'dest': DATA_TYPE_MAPPING[field_type],
            }
            fields['dimension'].append(field_info)
        # unknown field, add to internal ignore list
        else:
            fields['ignore'].append(field_info)

    for key, indices in fields.iteritems():

        if len(indices) < 1:
            continue

        if key == 'ignore':
            label = 'ignored'
        elif key == 'group_by':
            label = 'grouped'
        else:
            continue

        print "The following fields will be %s:" % label

        for fld in indices:
            print "    %s" % fld['name']

    return fields

# TODO: commented out because no true hookups of GeoJSON support
'''
def guess_layer_spatial_ref(layer):

    extent = layer.GetExtent()

    # is decimal degree?
    # -180 <= X <= 180
    # -90 <= X <= 90
    if (
        extent[0] >= -180. and
        extent[1] <= 180. and
        extent[2] >= -90. and
        extent[3] <= 90.
    ):
        # assume WGS84
        return 4326

    # cannot guess, return zero
    return 0
'''

def _get_postgis_srid(proj4):

    try:

        cursor = DBConn.cursor()

        cursor.execute("""
SELECT
    srid
FROM spatial_ref_sys
WHERE proj4text = %s
        """, [proj4])

        if cursor.rowcount > 0:
            srid = cursor.fetchone()[0]
        else:
            srid = 0

    except psycopg2.Error:
        DBConn.rollback()
        return 0
    finally:
        cursor.close()

    return srid

def get_layer_srid(layer):

    srid = Config.get('srid', None)
    if srid is not None:
        return srid

    # initial GeoJSON spec assumed WGS84 (EPSG:4326)
    return 4326

    # TODO: support for GeoJSON CRS support
    '''
    srs = layer.GetSpatialRef()

    # no spatial reference system, attempt to guess
    if not srs:
        return guess_layer_spatial_ref(layer)

    # invalid spatial reference system
    elif srs.Validate() != 0:
        return 0

    # try to get the PostGIS SRID
    srid = _get_postgis_srid(srs.ExportToProj4())

    return srid
    '''

def extract_group(feat, fields):

    num_group_by = len(fields['group_by'])
    group_list = []
    group_dict = {}
    for idx in xrange(num_group_by):
        group_list.append(feat.GetField(fields['group_by'][idx]['index']))
        group_dict[fields['group_by'][idx]['name']] = group_list[-1]

    return group_dict

def convert_date_to_seconds(the_date):
    '''
    convert date to number of seconds UTC from UNIX epoch
    value is a decimal to capture milliseconds
    '''

    if not isinstance(the_date, datetime.date):

        # if datetime, convert to 
        if isinstance(the_date, datetime.datetime):
            if the_date.tzinfo is not None:
                the_date = the_date.astimezone(pytz.UTC)
            the_date = the_date.date()
        # no can do
        else:
            raise

    the_date = datetime.datetime.combine(
        the_date,
        datetime.time(0, 0, 0, tzinfo=pytz.UTC)
    )

    return (
        the_date -
        datetime.datetime(1970, 1, 1, tzinfo=pytz.UTC)
    ).total_seconds()

def convert_time_to_seconds(the_time):
    '''
    convert time to number of seconds UTC from 00:00:00 UTC
    value is a decimal to capture milliseconds
    '''

    if not isinstance(the_time, datetime.time):

        # if datetime, convert to 
        if isinstance(the_time, datetime.datetime):
            the_time = the_time.time()

            if the_time.tzinfo is None:
                raise

        # no can do
        else:
            raise

    return (
        the_time.astimezone(pytz.UTC) -
        datetime.time(0, 0, 0, tzinfo=pytz.UTC)
    ).total_seconds()

def convert_datetime_to_seconds(the_datetime):
    '''
    convert datetime to number of seconds UTC from UNIX epoch
    value is a decimal to capture milliseconds
    '''

    return (
        the_datetime.astimezone(pytz.UTC) -
        datetime.datetime(1970, 1, 1, tzinfo=pytz.UTC)
    ).total_seconds()

def build_pcpoint_from_feature(feat, fields, struct_format=False):

    geom = shape(feat['geometry'])
    if not isinstance(geom, Point):
        geom = geom.centroid

    localtz = Config.get('timezone')
    if localtz is None:
        localtz = get_localzone()

    properties = feat['properties']

    vals = []
    frmt = []
    for dimension in fields['dimension']:

        if struct_format:
            frmt.append(dimension['type']['dest']['struct'])

        # x, y, z dimension
        if dimension['name'] in COORDINATES:

            try:
                vals.append(getattr(geom, dimension['name'].lower()))
            except shapely.geos.DimensionError:
                vals.append(0.)

        # processing override
        elif 'treat_as' in dimension['type']:

            val = properties[dimension['name']]
            treat_as = dimension['type']['treat_as']
            callback = dimension['type']['callback']

            if treat_as == 'date':

                val = callback(val)
                vals.append(
                    convert_date_to_seconds(
                        val.date()
                    )
                )

            elif treat_as == 'time':

                val = callback(val).time()
                if val.tzinfo is None:
                    val = localtz.localize(val)

                vals.append(
                    convert_time_to_seconds(
                        val
                    )
                )

            elif treat_as == 'datetime':

                val = callback(val)
                if val.tzinfo is None:
                    val = localtz.localize(val)

                vals.append(
                    convert_datetime_to_seconds(
                        val
                    )
                )

        # standard behavior
        else:

            val = properties[dimension['name']]

            # cast data if needed
            func = dimension['type']['dest'].get('cast', None)
            if func is not None:
                val = func(val)

            vals.append(val)
        
    if struct_format:
        return vals, ' '. join(frmt)
    else:
        return vals

def import_layer(layer, file_table, pcid, fields):

    buffer_size = Config.get('buffer_size')
    copy_mode = Config.get('copy_mode')

    num_features = len(layer)

    # create temporary table for layer
    temp_table = create_temp_table(DBConn)

    frmt = None
    wkb_set = []

    # iterate over features
    for idx in xrange(num_features):

        feat = layer[idx]

        # get group
        group = extract_group(feat, fields)

        # build pcpoint values
        if frmt is None:
            vals, frmt = build_pcpoint_from_feature(feat, fields, True)
        else:
            vals = build_pcpoint_from_feature(feat, fields)

        # make wkb of pcpoint
        wkb_set.append(make_wkb_point(pcid, frmt, vals))

        if len(wkb_set) >= buffer_size:
            if copy_mode is True:
                copy_pcpoints(DBConn, temp_table, wkb_set, group)
            else:
                insert_pcpoints(DBConn, temp_table, wkb_set, group)
            wkb_set = []

    if len(wkb_set) >= 0:
        copy_pcpoints(DBConn, temp_table, wkb_set, group)
        wkb_set = []

    file_name = Config.get('input_file', None)
    if file_name:
        file_name = os.path.basename(file_name)

    # build patches for layer by distinct group
    insert_pcpatches(
        DBConn,
        file_table,
        temp_table,
        layer,
        Config.get('metadata', None),
        file_name
    )

    return True

def convert_layer(layer, file_name, file_table):

    # process fields
    # find what to group by, what to ignore, what to process
    fields = interpret_fields(layer)

    if not fields['dimension']:
        return

    # get SRID of layer
    srid = get_layer_srid(layer)

    # specified pcid
    pcid = Config.get('pcid', None)
    if pcid is None:
        # build pointcloud schema
        pc_schema = build_pc_schema(fields)

        # add schema to database
        pcid = add_pc_schema(DBConn, pc_schema, srid)

    # do the actual import
    import_layer(layer, file_table, pcid, fields)

    print 'File "%s" has been imported into Table "%s" with PCID "%s" and SRID "%s"' % (
        file_name,
        file_table,
        pcid,
        srid
    )

def convert_file():

    file_name = Config.get('input_file', None)
    table_name = Config.get('table_name', None)

    if table_name is None:
        table_name = '"' + os.path.splitext(os.path.basename(file_name))[0] + '"'
    else:
        # qualify
        table_name = '"' + '"."'.join(table_name.split('.', 1)) + '"'

    table_action = Config.get('table_action', 'c')
    if table_action is None:
        table_action = 'c'
    table_action = table_action[0]

    create_pcpatch_table(
        DBConn,
        table_name,
        table_action
    )

    layer = DSIn['features']

    if not layer:
        raise

    metadata = DSIn.get('properties', None)
    if metadata and not Config.get('metadata', None):
        Config['metadata'] = metadata

    convert_layer(layer, file_name, table_name)

def geojson_to_pgpointcloud(config):

    global Config
    global DSIn
    global DBConn

    Config = config
    DSIn = open_input_file(Config.get('input_file', None))
    DBConn = open_db_connection(Config.get('dsn', None))

    try:
        convert_file()
        DBConn.commit()
    except:
        DBConn.rollback()
        raise
    finally:
        DBConn.close()
