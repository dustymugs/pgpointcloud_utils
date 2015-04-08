import copy
import struct
import binascii
import pyproj
from decimal import Decimal
from numeric_string_parser import NumericStringParser

from .pcexception import *
from .pcformat import PcDimension, PcFormat

class PcPoint(object):

    _dimensions = []

    # header format
    #
    # byte (endian)
    # uint32 (pcid)
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
            raise PcInvalidArgException(
                message='Value not an instance of PcFormat'
            )

        self._pcformat = new_value

        # the number of possible values is driven by 
        num_dimensions = len(self._pcformat.dimensions)
        num_values = len(self._raw_values)
        if num_values < 1:
            self._raw_values = [0.] * num_dimensions
        elif num_values > num_dimensions:
            self._raw_values = self._raw_values[:num_dimensions]
        elif num_values < num_dimensions:
            self._raw_values += ([0.] * (num_dimensions - num_values))

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
            raise PcInvalidArgException(
                message='Value not a list'
            )

        dimensions = self.pcformat.dimensions
        num_dimensions = len(dimensions)
        if len(new_values) != num_dimensions:
            raise PcInvalidArgException(
                message='Value has different number of elements than PcFormat dimensions'
            )

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
            raise PcRunTimeException(
                message='Cannot dump PcPoint without a PcFormat'
            )

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
        return the value of provided dimension name or position (1-based)
        '''

        if self.pcformat is None:
            raise PcRunTimeException(
                message='Cannot get dimension value from PcPoint without PcFormat'
            )

        # get raw value
        if isinstance(name_or_pos, int):
            # position is 1-based
            raw_value = self._raw_values[name_or_pos - 1]
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
        set the value of provided dimension name or position (1-based)
        '''

        if self.pcformat is None:
            raise PcRunTimeException(
                message='Cannot set dimension value from PcPoint without PcFormat'
            )

        # scale if dimension has scale
        dim = self.pcformat.get_dimension(name_or_pos)
        if Decimal(dim.scale) != Decimal(PcDimension.DEFAULT_SCALE):
            raw_value = value / dim.scale
        else:
            raw_value = value

        if isinstance(name_or_pos, int): 
            # position is 1-based
            self._raw_values[name_or_pos - 1] = raw_value
        else:
            self._raw_values[self.pcformat.get_dimension_index(name_or_pos)] = raw_value

    def copy(self):
        '''
        returns a copy of this PcPoint
        '''

        pt = PcPoint(pcformat=self.pcformat)
        pt._raw_values = copy.deepcopy(self._raw_values)

        return pt

    def transform(self, pcformat, mapping):
        '''
        transform PcPoint to provided pcformat using the given mapping

        transforms by:
            1. converting values
            2. reprojecting coordinates (X,Y) if pcformat has different SRID

        returns new PcPoint
        '''

        # if From pcformat == To pcformat, return PcPoint
        if self.pcformat == pcformat:
            return self.copy()

        # get info of From pcformat
        from_dimensions = self.pcformat.dimensions
        num_from_dimensions = len(from_dimensions)

        # get info of To pcformat
        to_dimensions = pcformat.dimensions
        num_to_dimensions = len(to_dimensions)

        # load mapping
        if not isinstance(mapping, dict):
            raise PcInvalidArgException(
                message='mapping not a dict'
            )

        # new pcpoint
        to_pcpoint = PcPoint(pcformat=pcformat)

        #
        # run conversion
        #

        # placeholders for if expressions are needed
        expr_assignment = {}
        nsp = None

        to_values = [0.] * num_to_dimensions
        map_keys = mapping.keys()
        for to_idx in xrange(num_to_dimensions):

            to_dimension = to_dimensions[to_idx]
            to_position = to_idx + 1
            by_position = False

            # position match
            if to_position in map_keys:

                map_from = mapping[to_position]
                by_position = True

            # name match
            elif to_dimension.name in map_keys:

                map_from = mapping[to_dimension.name]

            # no match, exception
            else:

                raise PcInvalidArgException(
                    message='Destination PcFormat dimension "{dimension}" at position {position} not found in mapping'.format(
                        dimension=to_dimension.name,
                        position=to_position
                    )
                )

            # inspect map_from
            # None, use the "to"
            if map_from is None:

                if by_position:

                    to_values[to_idx] = self.get_value(to_position)

                else:

                    to_values[to_idx] = self.get_value(to_dimension.name)

            # integer, use as index
            elif isinstance(map_from, int):

                to_values[to_idx] = self.get_value(map_from)

            # string, use as dimension name
            elif isinstance(map_from, str):

                to_values[to_idx] = self.get_value(map_from)

            # dictionary, more advanced behavior
            elif isinstance(map_from, dict):

                if map_from.has_key('value'):

                    to_values[to_idx] = map_from.get('value')

                elif map_from.has_key('expression'):

                    expr = map_from.get('expression')

                    # assignment object does not exist
                    if len(expr_assignment) < 1:

                        for from_idx in xrange(num_from_dimensions):
                            from_position = from_idx + 1

                            from_value = str(self.get_value(from_position))
                            expr_assignment['$' + str(from_idx + 1)] = from_value
                            expr_assignment['$' + from_dimensions[from_idx].name] = from_value

                        # instance of NumericStringParser
                        nsp = NumericStringParser()

                    # substitute values for placeholders
                    for k, v in expr_assignment.iteritems():
                        expr = expr.replace(k, v)

                    # evaluate expression
                    to_values[to_idx] = nsp.eval(expr)

                else:
                    if by_position:
                        key = to_position
                    else:
                        key = to_dimension.name

                    raise PcInvalidArgException(
                        message="Unrecognized dictionary for mapping key: {key} ".format(
                            key=key
                        )
                    )

            else:
                if by_position:
                    key = to_position
                else:
                    key = to_dimension.name

                raise PcInvalidArgException(
                    message="Unrecognized value for mapping key: {key} ".format(
                        key=key
                    )
                )

        # set values
        to_pcpoint.values = to_values

        # reproject if different srid
        if self.pcformat.srid != pcformat.srid:

            if (
                self.pcformat.proj4text is None or
                len(self.pcformat.proj4text) < 1 or
                pcformat.proj4text is None or
                len(pcformat.proj4text) < 1
            ):
                raise PcRunTimeException(
                    message='Cannot reproject coordinates. Missing proj4text'
                )

            try:
                from_proj = pyproj.Proj(self.pcformat.proj4text)
                to_proj = pyproj.Proj(pcformat.proj4text)
            except:
                raise PcRunTimeException(
                    message='Cannot reproject coordinates. Invalid proj4text'
                )

            to_x, to_y = pyproj.transform(
                from_proj,
                to_proj,
                to_pcpoint.get_value('X'),
                to_pcpoint.get_value('Y')
            )

            to_pcpoint.set_value('X', to_x)
            to_pcpoint.set_value('Y', to_y)

        return to_pcpoint
