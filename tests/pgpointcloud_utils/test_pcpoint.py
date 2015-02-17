import unittest
import struct

from pgpointcloud_utils import PcDimension, PcFormat, PcPoint

class TestPcPoint(unittest.TestCase):

    def setUp(self):
        super(TestPcPoint, self).setUp()

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

        self.pcid = 1
        self.srid = 4326

        self.pcformat = PcFormat.import_format(
            pcid=self.pcid,
            srid=self.srid,
            schema=self.schema
        )

        self.hexstr = '010100000064CEFFFF94110000703000000400'.upper()

    def test_extract_pcid_from_binary(self):
        hexstrs = {
            1: '010100000064CEFFFF94110000703000000400',
            65507: '01E3FF0000ED61A9E5114958C0ED61A9E5114958C0ED61A9E5114958C0ED61A9E5114958C0ED61A9E5114958C0ED61A9E5114958C0ED61A9E5114958C0EDED61A9E5114958C0ED61A9E5114958C0ED61A9E5114958C0ED61A9E5114958C0ED61A9E5114958C0'
        }

        for pcid, hexstr in hexstrs.iteritems():
            self.assertEqual(
                PcPoint.extract_pcid_from_hex(hexstr),
                pcid
            )

    def test_from_hex(self):

        pt = PcPoint.from_hex(pcformat=self.pcformat, hexstr=self.hexstr)
        self.assertIsInstance(pt, PcPoint)
        self.assertEqual(pt.pcformat, self.pcformat)
        self.assertEqual(pt.pcformat.pcid, self.pcid)
        self.assertEqual(len(pt.values), len(self.pcformat.dimensions))

    def test_as_hex(self):

        pt = PcPoint.from_hex(pcformat=self.pcformat, hexstr=self.hexstr)
        self.assertEqual(pt.as_hex().upper(), self.hexstr)

    def test_get_dimension(self):

        pt = PcPoint.from_hex(pcformat=self.pcformat, hexstr=self.hexstr)
        self.assertEqual(pt.get_dimension('X'), -12700.)
        self.assertEqual(pt.get_dimension('Y'), 4500.)
        self.assertEqual(pt.get_dimension('Z'), 12400.)
        self.assertEqual(pt.get_dimension('Intensity'), 4.)

    def test_set_dimension(self):

        pt = PcPoint.from_hex(pcformat=self.pcformat, hexstr=self.hexstr)
        pt.set_dimension('Intensity', 999.)
        self.assertEqual(pt.get_dimension('Intensity'), 999.)
