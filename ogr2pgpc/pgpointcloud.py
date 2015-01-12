from osgeo import ogr

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
        'interpretation': 'int64_t',
        'size': 8
    },
    ogr.OFTTime: {
        'interpretation': 'int64_t',
        'size': 8
    },
    ogr.OFTDateTime: {
        'interpretation': 'int64_t',
        'size': 8
    }
}

