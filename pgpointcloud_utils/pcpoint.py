import struct
import binascii
from decimal import Decimal

from .pcformat import PcDimension, PcFormat

class PcPoint(object):

    _dimensions = []

    _HEADER_FORMAT = ['B', 'I']

    def __init__(
        self,
        pcformat, values=None
    ):

        self._pcformat = None
        self._raw_values = []

        if pcformat is not None:
            self.pcformat = pcformat
        if values is not None:
            self.values = values

    @property
    def pcformat(self):
        return self._pcformat

    @pcformat.setter
    def pcformat(self, new_value):

        if not isinstance(new_value, PcFormat):
            raise

        self._pcformat = new_value

        # the number of possible values is driven by 
        num_dimensions = len(self._pcformat.dimensions)
        num_values = len(self._raw_values)
        if num_values < 1:
            self._raw_values = [0. for x in xrange(num_dimensions)]
        elif num_values > num_dimensions:
            self._raw_values = self._raw_values[:num_dimensions]
        elif num_values < num_dimensions:
            self._raw_values += [0. for x in xrange(num_dimensions - num_values)]

    @staticmethod
    def _compute_processed_value(value, dimension):

        if Decimal(dimension.scale) != Decimal(PcDimension.DEFAULT_SCALE):
            return value * dimension.scale
        else:
            return value

    @staticmethod
    def _compute_raw_value(value, dimension):

        if Decimal(dimension.scale) != Decimal(PcDimension.DEFAULT_SCALE):
            return value / dimension.scale
        else:
            return value

    @property
    def values(self):
        '''
        return processed values. raw values are never returned
        '''

        return map(
            PcPoint._compute_processed_value,
            self._raw_values,
            self.pcformat.dimensions
        )

    @values.setter
    def values(self, new_values):
        '''
        set raw values by converting provided values
        '''

        if not isinstance(new_values, list):
            raise

        dimensions = self.pcformat.dimensions
        num_dimensions = len(dimensions)
        if len(new_values) != num_dimensions:
            raise

        self._raw_values = map(
            PcPoint._compute_raw_value,
            new_values,
            dimensions
        )

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
    def combined_format(cls, is_ndr, pcformat):

        header_format = cls.header_format(is_ndr)
        data_format = pcformat.struct_format

        return ' '.join([header_format, data_format])

    @classmethod
    def extract_pcid_from_binary(cls, data):

        s = struct.Struct(cls.header_format(
            is_ndr=cls.is_ndr(data)
        ))

        header = s.unpack(data[:s.size])
        return header[1]

    @classmethod
    def extract_pcid_from_hex(cls, hexstr):

        return cls.extract_pcid_from_binary(binascii.unhexlify(hexstr))

    @classmethod
    def from_binary(cls, pcformat, data):
        '''
        deserialize PcPoint from binary representation. returns tuple
        '''

        s = struct.Struct(cls.combined_format(
            is_ndr=cls.is_ndr(data),
            pcformat=pcformat
        ))

        values = [v for v in s.unpack(data)]

        pt = PcPoint(pcformat=pcformat)
        pt._raw_values = values[len(PcPoint._HEADER_FORMAT):]

        return pt

    @classmethod
    def from_hex(cls, pcformat, hexstr):
        '''
        deserialize PcPoint from hex representation. returns tuple
        '''

        return cls.from_binary(pcformat, binascii.unhexlify(hexstr))


    def as_binary(self):
        '''
        serialize PcPoint. returns binary representation
        '''

        if self.pcformat is None:
            raise

        s = struct.Struct(PcPoint.combined_format(
            is_ndr=True,
            pcformat=self.pcformat
        ))
        values = [1, self.pcformat.pcid] + self._raw_values

        return s.pack(*values)

    def as_hex(self):
        '''
        serialize PcPoint. returns hex representation
        '''

        return binascii.hexlify(self.as_binary())

    def get_value(self, name_or_pos):
        '''
        return the value of provided dimension name or position
        '''

        if self.pcformat is None:
            raise

        # get raw value
        if isinstance(name_or_pos, int):
            raw_value = self._raw_values[name_or_pos]
        else:
            raw_value = self._raw_values[self.pcformat.get_dimension_index(name_or_pos)]

        dim = self.pcformat.get_dimension(name_or_pos)
        if Decimal(dim.scale) != Decimal(PcDimension.DEFAULT_SCALE):
            value = raw_value * dim.scale
        else:
            value = raw_value

        return value

    def set_value(self, name_or_pos, value):
        '''
        set the value of provided dimension name or position
        '''

        if self.pcformat is None:
            raise

        # scale if dimension has scale
        dim = self.pcformat.get_dimension(name_or_pos)
        if Decimal(dim.scale) != Decimal(PcDimension.DEFAULT_SCALE):
            raw_value = value / dim.scale
        else:
            raw_value = value

        if isinstance(name_or_pos, int): 
            self._raw_values[name_or_pos] = raw_value
        else:
            self._raw_values[self.pcformat.get_dimension_index(name_or_pos)] = raw_value
