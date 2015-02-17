import unittest
import struct

from pgpointcloud_utils import PcDimension, PcFormat

class TestPcDimension(unittest.TestCase):

    def test_init(self):

        dim = PcDimension()

        self.assertTrue(hasattr(dim, '_name'))
        self.assertTrue(hasattr(dim, '_size'))
        self.assertTrue(hasattr(dim, '_interpretation'))
        self.assertTrue(hasattr(dim, '_scale'))

        self.assertIsNone(dim._name)
        self.assertIsNone(dim._size)
        self.assertIsNone(dim._interpretation)
        self.assertEqual(dim._scale, 1.)

        dim = PcDimension(name='name', size=2, interpretation='uint16_t', scale=0.1)
        self.assertEqual(dim._name, 'name')
        self.assertEqual(dim._size, 2)
        self.assertEqual(dim._interpretation, 'uint16_t')
        self.assertEqual(dim._scale, 0.1)

    def test_name(self):

        dim = PcDimension(name='name')
        self.assertEqual(dim.name, 'name')
        dim.name = 'new name'
        self.assertEqual(dim.name, 'new name')

    def test_size(self):

        dim = PcDimension(size=2)
        self.assertEqual(dim.size, 2)
        dim.size = 4
        self.assertEqual(dim.size, 4)

    def test_interpretation(self):

        dim = PcDimension(interpretation='uint16_t')
        self.assertEqual(dim.interpretation, 'uint16_t')
        dim.interpretation = 'uint8_t'
        self.assertEqual(dim.interpretation, 'uint8_t')

    def test_scale(self):

        dim = PcDimension(scale=1.)
        self.assertEqual(dim.scale, 1.)
        dim.scale = 0.001
        self.assertEqual(dim.scale, 0.001)

    def test_struct_format(self):

        dim = PcDimension(interpretation='uint16_t')
        self.assertEqual(
            dim.struct_format,
            PcDimension.INTERPRETATION_MAPPING['uint16_t'].get('struct', None)
        )

class TestPcFormat(unittest.TestCase):

    def setUp(self):
        super(TestPcFormat, self).setUp()

        self.schema = """<?xml version="1.0" encoding="UTF-8"?>
<pc:PointCloudSchema xmlns:pc="http://pointcloud.org/schemas/PC/1.1" 
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <pc:dimension>
    <pc:position>1</pc:position>
    <pc:size>4</pc:size>
    <pc:description>X coordinate as a long integer. You must use the 
                    scale and offset information of the header to 
                    determine the double value.</pc:description>
    <pc:name>X</pc:name>
    <pc:interpretation>int32_t</pc:interpretation>
    <pc:scale>0.01</pc:scale>
  </pc:dimension>
  <pc:dimension>
    <pc:position>2</pc:position>
    <pc:size>4</pc:size>
    <pc:description>Y coordinate as a long integer. You must use the 
                    scale and offset information of the header to 
                    determine the double value.</pc:description>
    <pc:name>Y</pc:name>
    <pc:interpretation>int32_t</pc:interpretation>
    <pc:scale>0.01</pc:scale>
  </pc:dimension>
  <pc:dimension>
    <pc:position>3</pc:position>
    <pc:size>4</pc:size>
    <pc:description>Z coordinate as a long integer. You must use the 
                    scale and offset information of the header to 
                    determine the double value.</pc:description>
    <pc:name>Z</pc:name>
    <pc:interpretation>int32_t</pc:interpretation>
    <pc:scale>0.01</pc:scale>
  </pc:dimension>
  <pc:dimension>
    <pc:position>4</pc:position>
    <pc:size>2</pc:size>
    <pc:description>The intensity value is the integer representation 
                    of the pulse return magnitude. This value is optional 
                    and system specific. However, it should always be 
                    included if available.</pc:description>
    <pc:name>Intensity</pc:name>
    <pc:interpretation>uint16_t</pc:interpretation>
    <pc:scale>1</pc:scale>
  </pc:dimension>
  <pc:metadata>
    <Metadata name="compression">dimensional</Metadata>
  </pc:metadata>
</pc:PointCloudSchema>
"""

    def test_init(self):

        pcformat = PcFormat()
        self.assertTrue(hasattr(pcformat, '_pcid'))
        self.assertTrue(hasattr(pcformat, '_srid'))
        self.assertTrue(hasattr(pcformat, '_dimensions'))

        pcformat = PcFormat(pcid=1, srid=1, dimensions=[])
        self.assertEqual(pcformat._pcid, 1)
        self.assertEqual(pcformat._srid, 1)
        self.assertEqual(pcformat._dimensions, [])

    def test_pcid(self):

        pcformat = PcFormat(pcid=1)
        self.assertEqual(pcformat.pcid, 1)
        pcformat.pcid = 2
        self.assertEqual(pcformat.pcid, 2)

    def test_srid(self):

        pcformat = PcFormat(srid=1)
        self.assertEqual(pcformat.srid, 1)
        pcformat.srid = 2
        self.assertEqual(pcformat.srid, 2)

    def test_dimensions(self):

        pcformat = PcFormat(dimensions=[])
        self.assertEqual(pcformat.dimensions, [])

        dimensions = [
            PcDimension()
        ]

        pcformat.dimensions = dimensions
        self.assertEqual(pcformat.dimensions, dimensions) 

    def test_import_pcformat(self):

        pcformat = PcFormat.import_format(
            pcid=1,
            srid=4326,
            schema=self.schema
        )
        self.assertIsInstance(pcformat, PcFormat)
        self.assertEqual(pcformat.pcid, 1)
        self.assertEqual(pcformat.srid, 4326)
        self.assertEqual(len(pcformat.dimensions), 4)
        for dim in pcformat.dimensions:
            self.assertIsInstance(dim, PcDimension)

    def test_struct_format(self):

        pcformat = PcFormat.import_format(
            pcid=1,
            srid=4326,
            schema=self.schema
        )

        s = pcformat.struct_format
        self.assertEqual(s, 'i i i H')

    def test_get_dimension_index(self):

        pcformat = PcFormat.import_format(
            pcid=1,
            srid=4326,
            schema=self.schema
        )

        self.assertEqual(pcformat.get_dimension_index('X'), 0)
        self.assertEqual(pcformat.get_dimension_index('Y'), 1)
        self.assertEqual(pcformat.get_dimension_index('Z'), 2)
        self.assertEqual(pcformat.get_dimension_index('Intensity'), 3)
