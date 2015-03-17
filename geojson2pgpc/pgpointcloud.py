import psycopg2
from psycopg2.extensions import AsIs

import datetime
from xml.etree import ElementTree as ETree

import math
import random
import simplejson as json

from cStringIO import StringIO
import struct
import binascii

from pgpointcloud_utils import PcRunTimeException, PcInvalidArgException

# mapping between OGR datatypes and pgPointCloud datatypes
DATA_TYPE_MAPPING = {
    bool: {
        'interpretation': 'uint8_t',
        'size': 8,
        'struct': 'B'
    },
    int: {
        'interpretation': 'double',
        'size': 8,
        'cast': float,
        'struct': 'd'
    },
    float: {
        'interpretation': 'double',
        'size': 8,
        'struct': 'd'
    },
    datetime.date: {
        'interpretation': 'double',
        'size': 8,
        'struct': 'd'
    },
    datetime.time: {
        'interpretation': 'double',
        'size': 8,
        'struct': 'd'
    },
    datetime.datetime: {
        'interpretation': 'double',
        'size': 8,
        'struct': 'd'
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
        datetime.date,
        datetime.time,
        datetime.datetime,
    ]:
        pc_description = ETree.Element('pc:description')
        pc_dimension.append(pc_description)

        if dimension['type']['source'] == datetime.date:
            pc_description.text = 'date as number of seconds UTC from UNIX epoch to 00:00:00 of the date'
        elif dimension['type']['source'] == datetime.time:
            pc_description.text = 'time as number of seconds UTC from 00:00:00'
        elif dimension['type']['source'] == datetime.datetime:
            pc_description.text = 'datetime as number of seconds UTC from UNIX epoch'

    pc_metadata = ETree.Element('pc:metadata')
    pc_dimension.append(pc_metadata)

    # additional tags indicating that dimension is special 
    # date, time, datetime
    python_ = ETree.Element('python')
    pc_metadata.append(python_)

    data_type = ETree.Element('datatype')
    python_.append(data_type)
    data_type.text = str(dimension['type']['source'])

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

def add_pc_schema(dbconn, pc_schema, srid=0):

    try:

        cursor = dbconn.cursor()

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

        # next best PCID
        cursor.execute("""
SELECT
	max(avail)
FROM generate_series(1, 65535) avail
LEFT JOIN pointcloud_formats used
	ON avail = used.pcid
WHERE used.pcid IS NULL
        """)
        if cursor.rowcount > 0:
            pcid = cursor.fetchone()[0]
        else:
            raise PcRunTimeException(
                message='Query error getting the next available PCID'
            )

        cursor.execute("""
INSERT INTO pointcloud_formats (pcid, srid, schema)
VALUES (%s, %s, %s)
        """, (
            pcid,
            srid,
            pc_schema
        ))

    except psycopg2.Error:
        dbconn.rollback()
        return None
    finally:
        cursor.close()

    return pcid

def create_pcpatch_table(dbconn, table_name, table_action):

    try:

        cursor = dbconn.cursor()

        # append to existing table, check that table exists
        if table_action == 'a':
            try:
                cursor.execute("""
SELECT 1 FROM %s
                """, [AsIs(table_name)])
            except psycopg2.Error:
                raise PcInvalidArgException(
                    message='Table not found: %s' % table_name
                )

            return

        # drop table
        if table_action == 'd':
            cursor.execute("""
DROP TABLE IF EXISTS %s
            """, [AsIs(table_name)])

        cursor.execute("""
CREATE TABLE %s (
    id BIGSERIAL PRIMARY KEY,
    pa PCPATCH,
    layer_name TEXT,
    file_name TEXT,
    group_by JSON,
    metadata JSON
)
        """, [AsIs(table_name)])

    except psycopg2.Error:
        dbconn.rollback()
        raise PcRunTimeException(
            message='Query error creating PcPatch table'
        )
    finally:
        cursor.close()

def make_wkb_point(pcid, frmt, vals):

    values = [1, pcid] + vals
    s = struct.Struct('< B I' + frmt)

    return binascii.hexlify(s.pack(*values))

def insert_pcpoints(dbconn, table_name, wkb_set, group):

    group_str = json.dumps(group)

    values = [
        [wkb, group_str]
        for wkb in wkb_set
    ]

    try:

        cursor = dbconn.cursor()

        statement = """
INSERT INTO %s (pt, group_by)
VALUES (%%s::pcpoint, %%s)
        """ % (
            AsIs(table_name)
        )

        cursor.executemany(
            statement,
            values
        )

    except psycopg2.Error:
        dbconn.rollback()
        raise PcRunTimeException(
            message='Query error inserting PcPoints'
        )
    finally:
        cursor.close()

    return True

def copy_pcpoints(dbconn, table_name, wkb_set, group):

    group_str = json.dumps(group)

    f = StringIO(
        '\n'.join([
            '\t'.join([wkb, group_str])
            for wkb in wkb_set
        ])
    )

    try:

        cursor = dbconn.cursor()

        cursor.copy_from(f, table_name, columns=('pt', 'group_by'))

    except psycopg2.Error:
        dbconn.rollback()
        raise PcRunTimeException(
            message='Query error copying PcPoints'
        )
    finally:
        cursor.close()

    return True

def get_extent_corners(cursor, table_name, in_utm=True):
    if not in_utm:
        cursor.execute("""
WITH extent AS (
    SELECT
        ST_Envelope(ST_Collect(pt::geometry)) AS shp
    FROM %s
)
SELECT
    ST_XMin(shp),
    ST_YMax(shp),
    ST_XMax(shp),
    ST_YMin(shp)
FROM extent
        """ % (
            AsIs(table_name)
        ))
    else:
        cursor.execute("""
WITH raw_extent AS (
    SELECT
        ST_Envelope(ST_Collect(pt::geometry)) AS shp
    FROM %s
), utmzone AS (
    SELECT
        utmzone(ST_Centroid(shp)) AS srid
    FROM raw_extent
), extent AS (
    SELECT
        ST_Transform(shp::geometry, srid) AS shp
    FROM raw_extent
    JOIN utmzone
        ON true
)
SELECT
    ST_XMin(shp),
    ST_YMax(shp),
    ST_XMax(shp),
    ST_YMin(shp)
FROM extent
        """ % (
            AsIs(table_name)
        ))

    return cursor.fetchone()

def _compute_patch_size(dbconn, temp_table, max_points_per_patch=400):

    def get_patch_count(cursor, temp_table, dim, max_points):
        '''
        returns the number of patches whose point count > max_points
        '''

        cursor.execute("""
WITH raw_extent AS (
    SELECT
        ST_Envelope(ST_Collect(pt::geometry)) AS shp
    FROM %s
), utmzone AS (
    SELECT
        utmzone(ST_Centroid(shp)) AS srid
    FROM raw_extent
), points AS (
    SELECT
        ST_Transform(pt::geometry, srid) AS geom
    FROM %s
    JOIN utmzone
        ON true
), extent AS (
    SELECT
        ST_Transform(shp::geometry, srid) AS shp
    FROM raw_extent
    JOIN utmzone
        ON true
)
SELECT
    ST_Centroid(ST_Collect(geom)) AS shp,
    count(points.*) AS geom_count
FROM points
JOIN extent
    ON true
GROUP BY ST_SnapToGrid(geom, ST_XMin(extent.shp), ST_YMax(extent.shp), %s, %s)
HAVING count(points.*) > %s
        """ % (
            AsIs(temp_table),
            AsIs(temp_table),
            dim,
            dim,
            max_points
        ))

        return cursor.rowcount

    try:

        cursor = dbconn.cursor()

        ulx, uly, lrx, lry = get_extent_corners(cursor, temp_table)
        width = lrx - ulx
        height = uly - lry

        # starting patch size in meters (due to UTM zone usage)
        patch_size = int(max(width / 10., height / 10.))

        # no patch size, any patch size is valid
        if patch_size < 1:
            return 100

        old_patch_sizes = [0]
        old_patch_counts = [0]
        delta = None
        long_tail_count = 0

        while True:

            # patch size less than 1
            # means no reasonable patch size worked
            if patch_size < 1:

                # use largest patch_size that had
                # the least number of patches over max points per patch

                min_patch_count = min(old_patch_counts[1:])
                max_patch_size = -1

                for idx in xrange(len(old_patch_counts) -  1, 0, -1):
                    if (
                        old_patch_counts[idx] == min_patch_count and
                        old_patch_sizes[idx] > max_patch_size
                    ):
                        max_patch_size = old_patch_sizes[idx]

                patch_size = max_patch_size
                break

            patch_count = \
                get_patch_count(cursor, temp_table, patch_size, max_points_per_patch)

            if abs(patch_size - old_patch_sizes[-1]) <= 1:
                if patch_count == 0:
                    if long_tail_count >= 5:
                        patch_size = old_patch_sizes[-1]
                        break
                    elif patch_size > old_patch_sizes[-1]:
                        long_tail_count += 1
                elif old_patch_counts[-1] == 0:
                    patch_size = old_patch_sizes[-1]
                    break
            elif long_tail_count > 0 and patch_count > 0 and old_patch_counts[-1] == 0:
                patch_size = old_patch_sizes[-1]
                break

            delta = max(abs(patch_size - old_patch_sizes[-1]) / 2, 1)
            if patch_count > 0:
                delta *= -1

            old_patch_sizes.append(patch_size)
            patch_size += delta

            old_patch_counts.append(patch_count)

        cols = int(math.ceil(width / patch_size))
        rows = int(math.ceil(height / patch_size))

    except psycopg2.Error:
        dbconn.rollback()
        raise PcRunTimeException(
            message='Query error computing grid for PcPatches'
        )
    finally:
        cursor.close()

    return patch_size

def insert_pcpatches(
    dbconn, file_table, temp_table, layer,
    metadata=None, file_name=None, max_points_per_patch=400
):

    if metadata:
        # try to be nice with json metadata
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            pass

    try:

        patch_size = _compute_patch_size(dbconn, temp_table, max_points_per_patch)

        cursor = dbconn.cursor()

        cursor.execute("""
WITH raw_extent AS (
    SELECT
        ST_Envelope(ST_Collect(pt::geometry)) AS shp
    FROM %s
), utmzone AS (
    SELECT
        utmzone(ST_Centroid(shp)) AS srid
    FROM raw_extent
), points AS (
    SELECT
        ST_Transform(pt::geometry, srid) AS geom,
        pt,
        group_by
    FROM %s
    JOIN utmzone
        ON true
), extent AS (
    SELECT
        ST_Transform(shp::geometry, srid) AS shp
    FROM raw_extent
    JOIN utmzone
        ON true
)
INSERT INTO %s (layer_name, file_name, group_by, metadata, pa) 
SELECT
    layer_name,
    %s,
    group_by::json,
    %s::json,
    pa
FROM (
    SELECT
        %s AS layer_name,
        group_by,
        PC_Patch(pt) AS pa
    FROM points
    JOIN extent
        ON true
    GROUP BY group_by, ST_SnapToGrid(geom, ST_XMin(extent.shp), ST_YMax(extent.shp), %s, %s)
) sub
        """, [
            AsIs(temp_table),
            AsIs(temp_table),
            AsIs(file_table),
            file_name,
            json.dumps(metadata),
            None,
            patch_size,
            patch_size
        ])

    except psycopg2.Error:
        dbconn.rollback()
        raise PcRunTimeException(
            message='Query error inserting PcPatches'
        )
    finally:
        cursor.close()

    return True

def create_temp_table(dbconn):

    table_name = (
        'temp_' +
        ''.join(random.choice('0123456789abcdefghijklmnopqrstuvwxyz') for i in range(16))
    )
    table_name = '"' + table_name + '"'

    try:

        cursor = dbconn.cursor()

        cursor.execute("""
CREATE TEMPORARY TABLE %s (
    id BIGSERIAL PRIMARY KEY,
    pt PCPOINT,
    group_by TEXT
)
ON COMMIT DROP;
        """, [AsIs(table_name)])

    except psycopg2.Error:
        dbconn.rollback()
        raise PcRunTimeException(
            message='Query error creating temporary PcPoint table'
        )
    finally:
        cursor.close()

    return table_name
