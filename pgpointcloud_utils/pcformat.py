from decimal import Decimal
import xml.etree.ElementTree as ET

from .pcexception import *

class PcDimension(object):

    DEFAULT_SCALE = 1.

    BYTE_1 = 1
    BYTE_2 = 2
    BYTE_4 = 4
    BYTE_8 = 8
    BYTES = [BYTE_1, BYTE_2, BYTE_4, BYTE_8]

    INTERPRETATION_MAPPING = {
        'unknown': {},
        'int8_t': {
            'size': BYTE_1,
            'struct': 'b'
        },
        'uint8_t': {
            'size': BYTE_1,
            'struct': 'B'
        },
        'int16_t': {
            'size': BYTE_2,
            'struct': 'h'
        },
        'uint16_t': {
            'size': BYTE_2,
            'struct': 'H'
        },
        'int32_t': {
            'size': BYTE_4,
            'struct': 'i'
        },
        'uint32_t': {
            'size': BYTE_4,
            'struct': 'I'
        },
        'int64_t': {
            'size': BYTE_8,
            'struct': 'q'
        },
        'uint64_t': {
            'size': BYTE_8,
            'struct': 'Q'
        },
        'float': {
            'size': BYTE_4,
            'struct': 'f'
        },
        'double': {
            'size': BYTE_8,
            'struct': 'd'
        },
    }
    INTERPRETATION = INTERPRETATION_MAPPING.keys()

    def __init__(
        self,
        name=None, size=None, interpretation=None, scale=None
    ):

        self._name = None
        self._size = None
        self._interpretation = None
        self._scale = PcDimension.DEFAULT_SCALE

        if name is not None:
            self.name = name
        if size is not None:
            self.size = size
        if interpretation is not None:
            self.interpretation = interpretation
        if scale is not None:
            self.scale = scale

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, new_value):
        try:
            new_value = str(new_value)
        except:
            raise PcInvalidArgException(
                message='Value cannot be treated as a string'
            )

        self._name = new_value

    @property
    def size(self):
        return self._size

    @size.setter
    def size(self, new_value):
        try:
            new_value = int(new_value)
        except:
            raise PcInvalidArgException(
                message='Value cannot be treated as an integer'
            )

        if new_value not in PcDimension.BYTES:
            raise PcInvalidArgException(
                message='Invalid size provided'
            )

        self._size = new_value

    @property
    def interpretation(self):
        return self._interpretation

    @interpretation.setter
    def interpretation(self, new_value):

        if new_value not in PcDimension.INTERPRETATION:
            raise PcInvalidArgException(
                message='Invalid interpretation provided'
            )

        self._interpretation = new_value

    @property
    def scale(self):
        return self._scale

    @scale.setter
    def scale(self, new_value):
        try:
            new_value = float(new_value)
        except:
            raise PcInvalidArgException(
                message='Value cannot be treated as an float'
            )

        # scale cannot be zero
        if Decimal(new_value) == Decimal(0.):
            raise PcInvalidArgException(
                message='Value cannot be zero'
            )

        self._scale = new_value

    @property
    def struct_format(self):

        if self.interpretation is None:
            return None

        return PcDimension.INTERPRETATION_MAPPING[self.interpretation].get(
            'struct', None
        )

class PcFormat(object):

    def __init__(self, pcid=None, srid=None, proj4text=None, dimensions=None):

        self._pcid = None
        self._srid = None
        self._proj4text = None
        self._dimensions = []
        self._dimension_lookup = {}

        if pcid:
            self.pcid = pcid

        if srid:
            self.srid = srid

        if dimensions:
            self.dimensions = dimensions

    @property
    def pcid(self):
        return self._pcid

    @pcid.setter
    def pcid(self, new_value):
        try:
            new_value = int(new_value)
        except:
            raise PcInvalidArgException(
                message='Value cannot be treated as an integer'
            )

        self._pcid = new_value

    @property
    def srid(self):
        return self._srid

    @srid.setter
    def srid(self, new_value):
        try:
            new_value = int(new_value)
        except:
            raise PcInvalidArgException(
                message='Value cannot be treated as an integer'
            )

        self._srid = new_value

    @property
    def proj4text(self):
        return self._proj4text

    @proj4text.setter
    def proj4text(self, new_value):
        try:
            new_value = str(new_value)
        except:
            raise PcInvalidArgException(
                message='Value cannot be treated as a string'
            )

        self._proj4text = new_value

    @property
    def dimensions(self):
        return self._dimensions

    @dimensions.setter
    def dimensions(self, new_value):

        if not isinstance(new_value, list):
            raise PcInvalidArgException(
                message='Value not a list'
            )

        for dim in new_value:
            if not isinstance(dim, PcDimension):
                raise PcInvalidArgException(
                    message='Element of list not instance of PcDimension'
                )

        self._dimensions = new_value

        # build lookups
        self._build_dimension_lookups()

    def _build_dimension_lookups(self):

        self._dimension_lookups = {
            'name': {}
        }

        for dim in self._dimensions:
            self._dimension_lookups['name'][dim.name] = dim

    @classmethod
    def import_format(cls, pcid, srid, schema):
        '''
        helper function to import record from pgpointcloud_formats table
        '''

        frmt = cls(pcid=pcid, srid=srid)

        namespaces = {
            'pc': 'http://pointcloud.org/schemas/PC/1.1'
        }
        root = ET.fromstring(schema)

        # first pass, build dict of dimensions
        dimensions = {}
        for dim in root.findall('pc:dimension', namespaces):
            position = int(dim.find('pc:position', namespaces).text) - 1
            size = dim.find('pc:size', namespaces).text
            name = dim.find('pc:name', namespaces).text
            interpretation = dim.find('pc:interpretation', namespaces).text
            scale = dim.find('pc:scale', namespaces)
            if scale is not None:
                scale = scale.text

            dimensions[position] = PcDimension(
                name=name,
                size=size,
                interpretation=interpretation,
                scale=scale
            )

        # second pass, convert dict to list for guaranteed order
        _dimensions = [None] * len(dimensions)
        for position, dimension in dimensions.iteritems():
            _dimensions[position] = dimension
        frmt.dimensions = _dimensions

        return frmt

    @property
    def struct_format(self):

        frmt = []
        num_dimensions = len(self.dimensions)
        for position in xrange(num_dimensions):
            frmt.append(
                self.dimensions[position].struct_format
            )

        frmt = ' '.join(frmt)
        return frmt

    def get_dimension(self, name_or_pos):
        '''
        return the dimension by name or position
        '''

        if isinstance(name_or_pos, int):
            # position is 1-based
            return self.dimensions[name_or_pos - 1]
        else:
            return self._dimension_lookups['name'][name_or_pos]

    def get_dimension_index(self, name):
        '''
        return the index of the dimension by name
        '''

        if name not in self._dimension_lookups['name']:
            return None

        return self.dimensions.index(self._dimension_lookups['name'][name])
