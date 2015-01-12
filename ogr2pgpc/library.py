import datetime
import pytz

import os

import simplejson as json
import hashlib
from xml.etree import ElementTree as ETree
import psycopg2
from psycopg2.extensions import AsIs
from osgeo import ogr, osr
import argparse

import random

import ipdb

from .ogr import OGR_TZ
from .pgpointcloud import DATA_TYPE_MAPPING

Config = {}
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
        'dimension': []
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
    for idx in xrange(numFields):
        fldDef = feat.GetFieldDefnRef(idx)

        fldInfo = {
            'index': idx,
            'name': fldDef.GetName(), 
        }

        fldType = fldDef.GetType()


        if group_by and fldInfo['name'] in group_by:
            fields['group_by'].append(fldInfo)
        elif fldType == ogr.OFTString:
            if not group_by:
                fields['group_by'].append(fldInfo)
            else:
                fields['ignore'].append(idx)
        elif fldType in DATA_TYPE_MAPPING:
            fldInfo['type'] = {
                'source': fldType,
                'dest': DATA_TYPE_MAPPING[fldType],
            }
            fields['dimension'].append(fldInfo)
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

def build_pc_dimension(doc, dimension, index):

    pc_dimension = ETree.Element('pc:dimension')
    doc.append(pc_dimension)

    pc_position = ETree.Element('pc:position')
    pc_dimension.append(pc_position)
    pc_position.text = str(index)

    pc_name = ETree.Element('pc:name')
    pc_dimension.append(pc_name)
    pc_name.text = dimension['name']

    pc_size = ETree.Element('pc:size')
    pc_dimension.append(pc_size)
    pc_size.text = str(dimension['type']['dest']['size'])

    pc_interpretation = ETree.Element('pc:interpretation')
    pc_dimension.append(pc_interpretation)
    pc_interpretation.text = dimension['type']['dest']['interpretation']

    if dimension['type']['source'] in [
        ogr.OFTDate,
        ogr.OFTTime,
        ogr.OFTDateTime
    ]:
        pc_description = ETree.Element('pc:description')
        pc_dimension.append(pc_description)

        if dimension['type']['source'] == ogr.OFTDate:
            pc_description.text = 'date as number of seconds UTC from UNIX epoch to 00:00:00 of the date'
        elif dimension['type']['source'] == ogr.OFTTime:
            pc_description.text = 'time as number of seconds UTC from 00:00:00'
        elif dimension['type']['source'] == ogr.OFTDateTime:
            pc_description.text = 'datetime as number of seconds UTC from UNIX epoch'

    pc_metadata = ETree.Element('pc:metadata')
    pc_dimension.append(pc_metadata)

    # additional tags indicating that dimension is special 
    # date, time, datetime
    ogr_ = ETree.Element('ogr')
    pc_metadata.append(ogr_)

    data_type = ETree.Element('datatype')
    ogr_.append(data_type)
    data_type.text = ogr.GetFieldTypeName(dimension['type']['source'])

def build_pc_schema(fields):
    XML_DECLARATION = '<?xml version="1.0" encoding="UTF-8"?>'

    pc_schema = ETree.Element('pc:PointCloudSchema')
    pc_schema.set('xmlns:pc', "http://pointcloud.org/schemas/PC/1.1")
    pc_schema.set('xmlns:xsi', "http://www.w3.org/2001/XMLSchema-instance")

    pc_metadata = ETree.Element('pc:metadata')
    pc_schema.append(pc_metadata)

    # compression
    Metadata = ETree.Element('Metadata')
    pc_metadata.append(Metadata)
    Metadata.set('name', 'compression')
    Metadata.text = 'dimensional'

    num_dimensions = 1

    for dimension in fields['dimension']:
        build_pc_dimension(pc_schema, dimension, num_dimensions)
        num_dimensions += 1

    return XML_DECLARATION + ETree.tostring(pc_schema)

def _add_pc_schema(pc_schema, srid):

    cursor = DBConn.cursor()

    try:

        # check if this schema already exists
        cursor.execute("""
SELECT
    pcid
FROM pointcloud_formats
WHERE schema = %s
        """, [pc_schema])
        # it does exist, use
        if cursor.rowcount > 0:
            return cursor.fetchone()[0]

        # see if there are any available PCIDs
        cursor.execute("""
SELECT count(*) FROM pointcloud_formats
        """)
        pcid_count = cursor.fetchone()[0]

        # no vacancy
        if pcid_count == 65535:
            raise

        # next best PCID
        cursor.execute("""
WITH foo AS (
SELECT
	pcid,
	lead(pcid, 1, NULL) OVER (ORDER BY pcid DESC),
	pcid - lead(pcid, 1, NULL) OVER (ORDER BY pcid DESC) AS dist
FROM pointcloud_formats
ORDER BY pcid DESC
)
SELECT
	foo.pcid - 1 AS next_pcid
FROM foo
WHERE (foo.dist > 1 OR foo.dist IS NULL)
	AND (SELECT count(*) FROM pointcloud_formats AS srs WHERE srs.pcid = foo.pcid - 1) < 1
ORDER BY foo.pcid DESC
LIMIT 1
        """)
        if cursor.rowcount > 0:
            next_pcid = cursor.fetchone()[0]
        else:
            next_pcid = 65535

        cursor.execute(
            'INSERT INTO pointcloud_formats (pcid, srid, schema) VALUES (%s, %s, %s)', (
                next_pcid,
                srid,
                pc_schema
            )
        )

    except psycopg2.Error:
        DBConn.rollback()
        return None
    finally:
        cursor.close()

    return next_pcid

def add_pc_schema(pc_schema, srid=0):

    max_count = 5
    count = 0

    pcid = None
    while pcid is None:
        count += 1
        pcid = _add_pc_schema(pc_schema, srid)

        if count > max_count:
            raise

    return pcid

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

def build_pcpoint_from_feature(feat, fields):

    geom = feat.geometry()
    if geom.GetGeometryType() != ogr.wkbPoint:
        geom = geom.Centroid()

    vals = []
    for dimension in fields['dimension']:

        if dimension['name'] in ['X', 'Y', 'Z']:
            func = getattr(geom, 'Get' + dimension['name'])
            vals.append(func())
        elif dimension['type']['source'] in [
            ogr.OFTDate,
            ogr.OFTTime,
            ogr.OFTDateTime
        ]:
            val = feat.GetFieldAsDateTime(dimension['index'])

            # date is converted to number of seconds UTC from UNIX epoch
            if dimension['type']['source'] == ogr.OFTDate:
                vals.append((
                    datetime.datetime(*val[0:3]) -
                    datetime.datetime(1970, 1, 1)
                ).total_seconds())
            # time is converted to number of seconds from 00:00:00 UTC
            elif dimension['type']['source'] == ogr.OFTTime:
                tz = OGR_TZ(val[-1])
                vals.append((
                    datetime.time(*val[3:6], tzinfo=tz).astimezone(pytz.UTC) -
                    datetime.time(0, 0, 0, tzinfo=pytz.UTC)
                ).total_seconds())
            elif dimension['type']['source'] == ogr.OFTDateTime:
                tz = OGR_TZ(val[-1])
                vals.append((
                    datetime.datetime(*val[0:6], tzinfo=tz).astimezone(pytz.UTC) -
                    datetime.datetime(1970, 1, 1)
                ).total_seconds())
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
        pcid = add_pc_schema(pc_schema, srid)

    # do the actual import
    import_layer(layer, file_table, pcid, fields)

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
