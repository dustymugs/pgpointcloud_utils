from osgeo import ogr
from xml.etree import ElementTree as ETree

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

