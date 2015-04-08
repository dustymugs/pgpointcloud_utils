import copy
import struct
import binascii
import pyproj
from decimal import Decimal
from numeric_string_parser import NumericStringParser

from .pcexception import *
from .pcformat import PcDimension, PcFormat

class PcPatc(object):

    # header format
    #
    # byte (endian)
    # uint32 (pcid)
    # uint32 (compression)
    # uint32 (npoints)
    _HEADER_FORMAT = ['B', 'I', 'I', 'I']

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
        return header[1]

    @classmethod
    def extract_pcid_from_hex(cls, hexstr):

        return cls.extract_pcid_from_binary(binascii.unhexlify(hexstr))

    @classmethod
    def extract_compression_from_binary(cls, data):

        header = cls.extract_header_from_binary(data)
        return header[2]

    @classmethod
    def extract_compression_from_hex(cls, hexstr):

        return cls.extract_compression_from_binary(binascii.unhexlify(hexstr))

    @classmethod
    def extract_npoints_from_binary(cls, data):

        header = cls.extract_header_from_binary(data)
        return header[3]

    @classmethod
    def extract_npoints_from_hex(cls, hexstr):

        return cls.extract_npoints_from_binary(binascii.unhexlify(hexstr))
