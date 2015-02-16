# PostgreSQL pgPointCloud Add-on functions

A collection of helper functions for interacting with PcPoints and PcPatches.

## Requirements

* plpython2u

## Functions

### PC_Transform

Transform a PcPoint from one schema to another

#### Signature

_pcpoint_ __PC_Transform__(pt _pcpoint_, pcid _integer_, mapping _json_)

#### Description

Transform a PcPoint from one schema to another by specifying the destination PCID and a JSON object that maps attributes between schemas.

#### Examples

```

# key is either the position or attribute name from the destination schema
# value is a JSON object describing the normalization to be done on the origin schema attribute
#

'''
pcid = 1 (srid = 4326)

X (double)
Y (double)
Z (double)
alpha (uint8_t)
bravo (float)
charlie (double)
'''

'''
pcid = 10 (srid = 4269)

X (int32_t, scale=0.01)
Y (int32_t, scale=0.01)
Z (int32_t, scale=0.01)
charlie (double)
bravo (double)
alpha (int16_t)
'''

'''
pcid = 20 (srid = 4269)

X (double)
Y (double)
Z (double)
yankee (uint32_t)
zulu (double)
'''

'''
SELECT PC_Transform(
    pt, # PcPoint
    10, # destination PCID,
    mapping # JSON object
)
'''

# from PCID 1 to 10
# a dict, so order is not guarenteed
mapping = {
    'srid': 'srid', # keyword to keyword, both SRIDs are found in pointcloud

    1: 1, # position to position
    'Y', 2, # keyword to position
    3: 'Z', # position to keyword
    'charlie': None, # None is special, means keyword applies to both sides
    'alpha': None,
    'bravo': None
}

# from PCID 1 to 10
# enhanced example
mapping = {
    'srid': None,

    1: '$X * 100.', # simple expression. coordinates are reprojected first
    'Y', 2,
    3: 'Z',
    'alpha': None,
    'bravo': None
    'charlie': None,
}

# from PCID 1 to 20
# advanced example
mapping = {
    'srid': None,

    'X': None,
    'Y': None,
    'Z': None,
    'yankee': {
        'value': 9999999
    }, # nothing maps, so value is explicitly set
    'zulu': '($alpha ** 2) + (2 * $bravo) - $charlie'
}
```
