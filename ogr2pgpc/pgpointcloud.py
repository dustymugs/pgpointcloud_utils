import psycopg2
from psycopg2.extensions import AsIs

from osgeo import ogr
from xml.etree import ElementTree as ETree

import random
import simplejson as json

# mapping between OGR datatypes and pgPointCloud datatypes
DATA_TYPE_MAPPING = {
    ogr.OFTInteger: {
        'interpretation': 'int64_t',
        'size': 8
    },
    ogr.OFTReal: {
        'interpretation': 'double',
        'size': 8
    },
    ogr.OFTDate: {
        'interpretation': 'double',
        'size': 8
    },
    ogr.OFTTime: {
        'interpretation': 'double',
        'size': 8
    },
    ogr.OFTDateTime: {
        'interpretation': 'double',
        'size': 8
    }
}

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

def _add_pc_schema(dbconn, pc_schema, srid):

    cursor = dbconn.cursor()

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
        dbconn.rollback()
        return None
    finally:
        cursor.close()

    return next_pcid

def add_pc_schema(dbconn, pc_schema, srid=0):

    max_count = 5
    count = 0

    pcid = None
    while pcid is None:
        count += 1
        pcid = _add_pc_schema(dbconn, pc_schema, srid)

        if count > max_count:
            raise

    return pcid

def create_pcpatch_table(dbconn, table_name, table_action):

    cursor = dbconn.cursor()

    try:

        # append to existing table, check that table exists
        if table_action == 'a':
            try:
                cursor.execute("""
SELECT 1 FROM "%s"
                """, [AsIs(table_name)])
            except psycopg2.Error:
                raise Exception('Table not found: %s' % table_name)

            return

        # drop table
        if table_action == 'd':
            cursor.execute("""
DROP TABLE IF EXISTS "%s"
            """, [AsIs(table_name)])

        cursor.execute("""
CREATE TABLE "%s" (
    id BIGSERIAL PRIMARY KEY,
    pa PCPATCH,
    layer_name TEXT,
    group_by JSON,
    metadata JSON
)
        """, [AsIs(table_name)])

    except psycopg2.Error:
        dbconn.rollback()
        raise
    finally:
        cursor.close()

def insert_pcpoint(dbconn, table_name, pcid, group, vals):

    cursor = dbconn.cursor()

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
        dbconn.rollback()
        raise
    finally:
        cursor.close()

    return True

def _compute_patch_size(dbconn, temp_table, max_points_per_patch=400):

    import pudb;pudb.set_trace()

    patch_size = 50
    size_step = 25
    within_count = 20

    def get_max_points(cursor, temp_table, dim):
        cursor.execute("""
WITH raw_extent AS (
	SELECT
		ST_Envelope(ST_Collect(pt::geometry)) AS shp
	FROM "%s"
), utmzone AS (
	SELECT
		utmzone(ST_Centroid(shp)) AS srid
	FROM raw_extent
), points AS (
	SELECT
		ST_Transform(pt::geometry, srid) AS pt
	FROM "%s"
	JOIN utmzone
		ON true
), extent AS (
	SELECT
		ST_Transform(shp::geometry, srid) AS shp
	FROM raw_extent
	JOIN utmzone
		ON true
), cells AS (
    SELECT
	    ST_Centroid(ST_Collect(pt)) AS shp,
    	count(points.*) AS pt_count
    FROM points
    JOIN extent
	    ON true
    GROUP BY ST_SnapToGrid(pt, ST_XMin(extent.shp), ST_YMax(extent.shp), %s, %s)
)
SELECT
    max(pt_count) AS max_pts
FROM cells
        """ % (
            AsIs(temp_table),
            AsIs(temp_table),
            dim,
            dim
        ))

        return cursor.fetchone()[0]

    def get_gradient(cursor, temp_table, dim, prior_max_points):
        max_points = get_max_points(cursor, temp_table, dim)
        delta = (max_points - prior_max_points) / float(prior_max_points)
        return delta, max_points

    patch_info = dict(
        x=None,
        y=None,
        width=None,
        height=None
    )

    cursor = dbconn.cursor()

    try:

        prior_max_points = get_max_points(cursor, temp_table, patch_size)

        delta = 1.
        while abs(prior_max_points - max_points_per_patch) > within_count:

            if delta == 0.:
                delta = 1.

            patch_size = int(patch_size - size_step * delta)

            delta, prior_max_points = get_gradient(cursor, temp_table, patch_size, prior_max_points)

    except psycopg2.Error:
        dbconn.rollback()
        raise
    finally:
        cursor.close()

    return patch_info

def insert_pcpatches(
    dbconn, file_table, temp_table, layer,
    metadata=None, max_points_per_patch=400
):

    layer_name = layer.GetName()

    cursor = dbconn.cursor()

    if metadata:
        # try to be nice with json metadata
        for idx in xrange(len(metadata)):
            try:
                metadata[idx] = json.loads(metadata[idx])
            except json.JSONDecodeError:
                pass

    try:

        cursor.execute("""
INSERT INTO "%s" (layer_name, group_by, metadata, pa) 
SELECT
    layer_name,
    group_by::json,
    %s::json,
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
            json.dumps(metadata),
            layer_name,
            AsIs(temp_table),
        ])

    except psycopg2.Error:
        dbconn.rollback()
        raise
    finally:
        cursor.close()

    return True

def create_temp_table(dbconn):

    table_name = (
        'temp_' +
        ''.join(random.choice('0123456789abcdefghijklmnopqrstuvwxyz') for i in range(16))
    )

    cursor = dbconn.cursor()

    try:

        '''
        cursor.execute("""
CREATE TEMPORARY TABLE "%s" (
    id BIGSERIAL PRIMARY KEY,
    pt PCPOINT,
    group_by TEXT
)
ON COMMIT DROP;
        """, [AsIs(table_name)])
        '''
        cursor.execute("""
CREATE TABLE "%s" (
    id BIGSERIAL PRIMARY KEY,
    pt PCPOINT,
    group_by TEXT
);
        """, [AsIs(table_name)])

    except psycopg2.Error:
        dbconn.rollback()
        raise
    finally:
        cursor.close()

    return table_name
