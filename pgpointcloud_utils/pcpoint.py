import struct
import binascii

from .pcformat import PcDimension, PcFormat

class PcPoint(object):

    _dimensions = []

    _HEADER_FORMAT = ['B', 'I']

    def __init__(
        self,
        pcformat, values=[]
    ):

        self._pcformat = None
        self._values = []

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

    @property
    def values(self):
        return self._values

    @values.setter
    def values(self, new_values):

        if not isinstance(new_values, list):
            raise

        self._values = new_values

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

        return PcPoint(
            pcformat=pcformat,
            values=values[len(PcPoint._HEADER_FORMAT):]
        )

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
        values = [1, self.pcformat.pcid] + self.values

        return s.pack(*values)

    def as_hex(self):
        '''
        serialize PcPoint. returns hex representation
        '''

        return binascii.hexlify(self.as_binary())

    def get_dimension(self, name):
        '''
        return the value of provided dimension name
        '''

        if self.pcformat is None:
            raise

        return self.values[self.pcformat.get_dimension_index(name)]

    def set_dimension(self, name, value):
        '''
        set the value of provided dimension name
        '''

        if self.pcformat is None:
            raise

        self.values[self.pcformat.get_dimension_index(name)] = value
