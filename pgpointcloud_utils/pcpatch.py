import warnings
import copy
import struct
import binascii
import pyproj
from decimal import Decimal
from numeric_string_parser import NumericStringParser

from .pcexception import *
from .pcformat import PcDimension, PcFormat

class PcPatch(object):

    # header format
    #
    # byte (endian)
    # uint32 (pcid)
    # uint32 (compression)
    # uint32 (npoints)
    _HEADER_FORMAT = ['B', 'I', 'I', 'I']

    HEADER_POS_PCID = 1
    HEADER_POS_COMPRESSION = 2
    HEADER_POS_NPOINTS = 3

    _compression = None
    _npoints = None
    _points = None
    _data = None

    UNCOMPRESSED = 'uncompressed'
    DIMENSIONAL = 'dimensional'
    GHT = 'ght'


    _COMPRESSION = {
        0: UNCOMPRESSED,
        1: GHT,
        2: DIMENSIONAL,
    }

    def __init__(self, pcformat, data):

        cls = self.__class__

        self._pcformat = None
        self._raw_values = []
        self._compression = None
        self._npoints = None
        self._points = None

        if pcformat is not None:
            self.pcformat = pcformat

        header = cls.extract_header_from_binary(data)
        self.compression = header[cls.HEADER_POS_COMPRESSION]

        if cls._COMPRESSION.get(self.compression, None) == cls.UNCOMPRESSED:
            # TODO: convert to array of PcPoint objects
            pass
        else:
            warnings.warn('Compressed patch detected. Cannot access points')
            self.npoints = header[cls.HEADER_POS_NPOINTS]
            self._data = data
        
    @classmethod
    def is_ndr(cls, data):
        '''
        get endian-ness
        True = NDR, False = XDR
        '''

        return bool(ord(data[0]))

    @classmethod
    def header_format(cls, is_ndr):

        if is_ndr:
            frmt = ['<']
        else:
            frmt = ['>']

        frmt += cls._HEADER_FORMAT

        return ' '.join(frmt)

    @classmethod
    def extract_header_from_binary(cls, data):

        s = struct.Struct(cls.header_format(
            is_ndr=cls.is_ndr(data)
        ))

        return s.unpack(data[:s.size])

    @classmethod
    def extract_header_from_hex(cls, hexstr):

        return cls.extract_header_from_binary(binascii.unhexlify(hexstr))

    @classmethod
    def extract_pcid_from_binary(cls, data):

        header = cls.extract_header_from_binary(data)
        return header[cls.HEADER_POS_PCID]

    @classmethod
    def extract_pcid_from_hex(cls, hexstr):

        return cls.extract_pcid_from_binary(binascii.unhexlify(hexstr))

    @classmethod
    def extract_compression_from_binary(cls, data):

        header = cls.extract_header_from_binary(data)
        return header[cls.HEADER_POS_COMPRESSION]

    @classmethod
    def extract_compression_from_hex(cls, hexstr):

        return cls.extract_compression_from_binary(binascii.unhexlify(hexstr))

    @classmethod
    def extract_npoints_from_binary(cls, data):

        header = cls.extract_header_from_binary(data)
        return header[cls.HEADER_POS_NPOINTS]

    @classmethod
    def extract_npoints_from_hex(cls, hexstr):

        return cls.extract_npoints_from_binary(binascii.unhexlify(hexstr))

    @classmethod
    def from_binary(cls, pcformat, data):
        '''
        deserialize PcPatch from binary representation. returns tuple

        data is deserialized only if patch is not compressed
        '''

        pt = PcPatch(pcformat=pcformat, data=data)
        pt._raw_values = values[len(PcPoint._HEADER_FORMAT):]

        return pt

    @classmethod
    def from_hex(cls, pcformat, hexstr):
        '''
        deserialize PcPoint from hex representation. returns tuple
        '''

        return cls.from_binary(pcformat, binascii.unhexlify(hexstr))

