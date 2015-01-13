import time
import datetime
from dateutil.parser import parse as datetime_parse
from tzlocal import get_localzone
import pytz

import os

import simplejson as json
#import hashlib
import psycopg2
from psycopg2.extensions import AsIs
from osgeo import ogr, osr
import argparse

import random

from .ogr import OGR_TZ
from .pgpointcloud import (
    DATA_TYPE_MAPPING,
    build_pc_dimension, build_pc_schema, add_pc_schema
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
    'group_by': [],
    'layer': [],
    'srid': None,
    'pcid': None,
    'table_name': None,
    'table_action': None,
    'date': [],
    'time': [],
    'datetime': [],
    'timezone': get_localzone(),
}
DSIn = None
DBConn = None

def open_input_file(f):

    global DSIn

    ogr.RegisterAll()

    DSIn = ogr.OpenShared(f, update=False)

    if DSIn is None:
        raise

    return DSIn

def open_db_connection(dsn):
    return psycopg2.connect(dsn)

def interpret_fields(layer):

    def add_coordinate(dimensions, coord):
        fldType = ogr.OFTReal
        dimensions.append({
            'index': None,
            'name': coord,
            'type': {
                'source': fldType,
                'dest': DATA_TYPE_MAPPING[fldType]
            }
        })

    fields = {
        'group_by': [],
        'ignore': [],
        'dimension': [],
        'overrides': {}
    }

    if layer.GetFeatureCount() < 1:
        raise

    add_coordinate(fields['dimension'], 'X')
    add_coordinate(fields['dimension'], 'Y')
    add_coordinate(fields['dimension'], 'Z')

    # use the first feature
    feat = layer.GetFeature(0)
    numFields = feat.GetFieldCount()

    group_by = Config.get('group_by', [])

    # date, time, datetime overrides
    overrides = {}
    for override_field, override_callback in OVERRIDE_INPUT_FORMAT:
        column = Config.get(override_field, [])

        overrides[override_field] = {
            'column': column,
            'callback': override_callback
        }

    # loop over each field
    for idx in xrange(numFields):
        fldDef = feat.GetFieldDefnRef(idx)

        fldInfo = {
            'index': idx,
            'name': fldDef.GetName(), 
        }

        fldType = fldDef.GetType()

        # user-defined group_by list and field in that list 
        if group_by and fldInfo['name'] in group_by:
            # add field to internal group_by list
            fields['group_by'].append(fldInfo)
        # field is string format
        elif fldType == ogr.OFTString:
            # field not in user-defined group_by list
            if not group_by:

                # field in override
                found = False
                for treat_as, details in overrides.iteritems():
                    if fldInfo['name'] in details['column']:
                        found = True
                        break

                if found:

                    fldInfo['type'] = {
                        'treat_as': treat_as,
                        'callback': details['callback'],
                        'source': None,
                        'dest': DATA_TYPE_MAPPING[fldType],
                    }
                    fields['dimension'].append(fldInfo)

                # default is to add to group_by
                else:
                    fields['group_by'].append(fldInfo)

            # add field to internal ignore list
            else:
                fields['ignore'].append(idx)
        # field is supported
        elif fldType in DATA_TYPE_MAPPING:
            fldInfo['type'] = {
                'source': fldType,
                'dest': DATA_TYPE_MAPPING[fldType],
            }
            fields['dimension'].append(fldInfo)
        # unknown field, add to internal ignore list
        else:
            fields['ignore'].append(fldInfo)

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

def _get_postgis_srid(proj4):

    cursor = DBConn.cursor()

    try:
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

def extract_group(feat, fields):

    num_group_by = len(fields['group_by'])
    group_list = []
    group_dict = {}
    for idx in xrange(num_group_by):
        group_list.append(feat.GetField(fields['group_by'][idx]['index']))
        group_dict[fields['group_by'][idx]['name']] = group_list[-1]

    #return hashlib.md5(json.dumps(group_list)).hexdigest(), group_dict
    return group_dict

def create_temp_table():

    table_name = (
        'temp_' +
        ''.join(random.choice('0123456789abcdefghijklmnopqrstuvwxyz') for i in range(16))
    )

    cursor = DBConn.cursor()

    try:

        cursor.execute("""
CREATE TEMPORARY TABLE "%s" (
    id BIGSERIAL PRIMARY KEY,
    pt PCPOINT,
    group_by TEXT
)
ON COMMIT DROP;
        """, [AsIs(table_name)])

    except psycopg2.Error:
        DBConn.rollback()
        raise
    finally:
        cursor.close()

    return table_name

def convert_date_to_seconds(the_date):
    '''
    convert date to number of seconds UTC from UNIX epoch
    '''

    return (
        the_date -
        datetime.datetime(1970, 1, 1)
    ).total_seconds()

def convert_time_to_seconds(the_time):
    '''
    convert time to number of seconds UTC from 00:00:00 UTC
    '''

    return (
        the_time.astimezone(pytz.UTC) -
        datetime.time(0, 0, 0, tzinfo=pytz.UTC)
    ).total_seconds()

def convert_datetime_to_seconds(the_datetime):
    '''
    convert datetime to number of seconds UTC from UNIX epoch
    '''

    return (
        the_datetime.astimezone(pytz.UTC) -
        datetime.datetime(1970, 1, 1)
    ).total_seconds()

def build_pcpoint_from_feature(feat, fields):

    geom = feat.geometry()
    if geom.GetGeometryType() != ogr.wkbPoint:
        geom = geom.Centroid()


    localtz = Config.get('timezone', get_localzone())

    vals = []
    for dimension in fields['dimension']:

        # x, y, z dimension
        if dimension['name'] in COORDINATES:

            func = getattr(geom, 'Get' + dimension['name'])
            vals.append(func())

        # OGR date, time or datetime
        elif dimension['type']['source'] in [
            ogr.OFTDate,
            ogr.OFTTime,
            ogr.OFTDateTime
        ]:

            val = feat.GetFieldAsDateTime(dimension['index'])

            if dimension['type']['source'] == ogr.OFTDate:

                vals.append(
                    convert_date_to_seconds(
                        datetime.datetime(*val[0:3])
                    )
                )

            elif dimension['type']['source'] == ogr.OFTTime:

                tz = OGR_TZ(val[-1])
                if tz.utcoffset() is None:
                    tz = localtz
                vals.append(
                    convert_time_to_seconds(
                        datetime.time(*val[3:6], tzinfo=tz)
                    )
                )

            elif dimension['type']['source'] == ogr.OFTDateTime:

                tz = OGR_TZ(val[-1])
                if tz.utcoffset() is None:
                    tz = localtz
                vals.append(
                    convert_datetime_to_seconds(
                        datetime.datetime(*val[0:6], tzinfo=tz)
                    )
                )

        elif (
            dimension['type']['source'] is None and
            'treat_as' in dimension['type']
        ):
            val = feat.GetField(dimension['index'])
            treat_as = dimension['type']['treat_as']
            callback = dimension['type']['callback']

            if treat_as == 'date':

                vals.append(
                    convert_date_to_seconds(
                        callback(val).date()
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

            vals.append(feat.GetField(dimension['index']))
        
    return vals

def insert_pcpoint(table_name, pcid, group, vals):

    cursor = DBConn.cursor()

    try:

        group_str = json.dumps(group)

        cursor.execute("""
INSERT INTO "%s" (pt, group_by)
VALUES (PC_MakePoint(%s, %s), %s)
        """, [
            AsIs(table_name),
            pcid,
            vals,
            group_str
        ])

    except psycopg2.Error:
        DBConn.rollback()
        raise
    finally:
        cursor.close()

def insert_pcpatches(file_table, temp_table, layer):

    layer_name = layer.GetName()

    cursor = DBConn.cursor()

    try:

        cursor.execute("""
INSERT INTO "%s" (layer_name, group_by, pa) 
SELECT
    layer_name,
    group_by::json,
    pa
FROM (
    SELECT
        %s AS layer_name,
        group_by,
        PC_Patch(pt) AS pa
    FROM "%s"
    GROUP BY 1, 2
) sub
        """, [
            AsIs(file_table),
            layer_name,
            AsIs(temp_table),
        ])

    except psycopg2.Error:
        DBConn.rollback()
        raise
    finally:
        cursor.close()

def import_layer(layer, file_table, pcid, fields):

    num_features = layer.GetFeatureCount()

    # create temporary table for layer
    temp_table = create_temp_table()

    # iterate over features
    for idx in xrange(num_features):

        feat = layer.GetFeature(idx)

        # get group
        group = extract_group(feat, fields)

        # build pcpoint values
        vals = build_pcpoint_from_feature(feat, fields)

        # insert
        insert_pcpoint(temp_table, pcid, group, vals)

    # build patches for layer by distinct group
    insert_pcpatches(file_table, temp_table, layer)

    return True

def convert_layer(layer, file_table):

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

    print 'Layer "%s" has been imported into Table "%s" with PCID "%s" and SRID "%s"' % (
        layer.GetName(),
        file_table,
        pcid,
        srid
    )

def create_file_table():

    table_name = Config.get('table_name', None)
    if table_name is None:
        table_name = os.path.splitext(os.path.basename(DSIn.name))[0]

    cursor = DBConn.cursor()

    action = Config.get('table_action', 'c')[0]

    try:

        # append to existing table, check that table exists
        if action == 'a':
            try:
                cursor.execute("""
SELECT 1 FROM "%s"
                """, [AsIs(table_name)])
            except psycopg2.Error:
                raise Exception('Table not found: %s' % table_name)

            return table_name

        # drop table
        if action == 'd':
            cursor.execute("""
DROP TABLE IF EXISTS "%s"
            """, [AsIs(table_name)])

        cursor.execute("""
CREATE TABLE "%s" (
    id BIGSERIAL PRIMARY KEY,
    pa PCPATCH,
    layer_name TEXT,
    group_by JSON
)
        """, [AsIs(table_name)])

    except psycopg2.Error:
        DBConn.rollback()
        raise
    finally:
        cursor.close()

    return table_name

def convert_file():

    file_table = create_file_table()

    num_layers = DSIn.GetLayerCount()
    if num_layers < 1:
        return

    layers = Config.get('layer', [])
    filtered_layers = True
    if not layers:
        layers = range(num_layers)
        filtered_layers = False

    for e in layers:

        if filtered_layers:
            layer = DSIn.GetLayerByName(e)
        else:
            layer = DSIn.GetLayerByIndex(e)

        if not layer:
            raise

        convert_layer(layer, file_table)

def ogr_to_pgpointcloud(config):

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
