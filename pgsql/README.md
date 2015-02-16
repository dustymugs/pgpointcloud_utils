# PostgreSQL pgPointCloud Add-on functions

A collection of helper functions for interacting with PcPoints and PcPatches.

## Requirements

* plpython2u

## Functions

### PC_Transform

Transform a PcPoint from one schema to another

#### Signature

pcpoint __PC_Transform__(_pt_ pcpoint, _pcid_ integer, _mapping_ json)

#### Description

Transform a PcPoint from one schema to another by specifying the destination PCID and a JSON object that maps attributes between schemas.

If the PcPoint's SRID differs from the destination PCID's SRID, the coordinates X, Y, Z will be projected.

The structure of _mapping_ is a JSON dictionary. Each key is a dimension position or name in the destination schema. The value is the position, name or object operating upon one or more dimensions of the PcPoint's schema.

* map source name to destination name

```
{
  ...
  'dest_key': 'origin_key',
  ...
}
```

* map source position to destination position

```
{
  ...
  5: 1,
  ...
}
```

* map source position to destination name

```
{
  ...
  'dest_key': 5
  ...
}
```

* map source name to destination position

```
{
  ...
  5: 'origin_key'
  ...
}
```

* map a constant to destination

```
{
  ...
  5: {
    'value': 65535
  },
  'dest_key': {
    'value': 65535
  }
  ...
}
```

* map an expression to destination

```
{
  ...
  5: {
    'expression': '$origin_key ** 2'
  },
  'dest_key': {
    'expression': '1. / $origin_key'
  }
  ...
}
```

_WARNING: Order is not guaranteed in JSON_

_WARNING: Though the same key can be provided in JSON, the last key will overwrite all prior instances of that key_

#### Example

There are three pointcloud schemas.

__PCID = 1 (SRID = 4326)__
```
<?xml version="1.0" encoding="UTF-8"?>
<pc:PointCloudSchema xmlns:pc="http://pointcloud.org/schemas/PC/1.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <pc:dimension>
    <pc:position>1</pc:position>
    <pc:name>X</pc:name>
    <pc:size>8</pc:size>
    <pc:interpretation>double</pc:interpretation>
  </pc:dimension>
  <pc:dimension>
    <pc:position>2</pc:position>
    <pc:name>Y</pc:name>
    <pc:size>8</pc:size>
    <pc:interpretation>double</pc:interpretation>
  </pc:dimension>
  <pc:dimension>
    <pc:position>3</pc:position>
    <pc:name>Z</pc:name>
    <pc:size>8</pc:size>
    <pc:interpretation>double</pc:interpretation>
  </pc:dimension>
  <pc:dimension>
    <pc:position>4</pc:position>
    <pc:name>alpha</pc:name>
    <pc:size>1</pc:size>
    <pc:interpretation>uint8_t</pc:interpretation>
  </pc:dimension>
  <pc:dimension>
    <pc:position>5</pc:position>
    <pc:name>bravo</pc:name>
    <pc:size>4</pc:size>
    <pc:interpretation>float</pc:interpretation>
  </pc:dimension>
  <pc:dimension>
    <pc:position>6</pc:position>
    <pc:name>charlie</pc:name>
    <pc:size>8</pc:size>
    <pc:interpretation>double</pc:interpretation>
  </pc:dimension>
  <pc:metadata>
    <Metadata name="compression">dimensional</Metadata>
  </pc:metadata>
</pc:PointCloudSchema>
```

__PCID = 2 (SRID = 4326)__
```
<?xml version="1.0" encoding="UTF-8"?>
<pc:PointCloudSchema xmlns:pc="http://pointcloud.org/schemas/PC/1.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <pc:dimension>
    <pc:position>1</pc:position>
    <pc:name>X</pc:name>
    <pc:size>4</pc:size>
    <pc:interpretation>int32_t</pc:interpretation>
    <pc:scale>0.01</pc:scale>
  </pc:dimension>
  <pc:dimension>
    <pc:position>2</pc:position>
    <pc:name>Y</pc:name>
    <pc:size>4</pc:size>
    <pc:interpretation>int32_t</pc:interpretation>
    <pc:scale>0.01</pc:scale>
  </pc:dimension>
  <pc:dimension>
    <pc:position>3</pc:position>
    <pc:name>Z</pc:name>
    <pc:size>4</pc:size>
    <pc:interpretation>int32_t</pc:interpretation>
    <pc:scale>0.01</pc:scale>
  </pc:dimension>
  <pc:dimension>
    <pc:position>4</pc:position>
    <pc:name>charlie</pc:name>
    <pc:size>8</pc:size>
    <pc:interpretation>double</pc:interpretation>
  </pc:dimension>
  <pc:dimension>
    <pc:position>5</pc:position>
    <pc:name>bravo</pc:name>
    <pc:size>4</pc:size>
    <pc:interpretation>float</pc:interpretation>
  </pc:dimension>
  <pc:dimension>
    <pc:position>6</pc:position>
    <pc:name>alpha</pc:name>
    <pc:size>1</pc:size>
    <pc:interpretation>uint8_t</pc:interpretation>
  </pc:dimension>
  <pc:metadata>
    <Metadata name="compression">dimensional</Metadata>
  </pc:metadata>
</pc:PointCloudSchema>
```

__PCID = 3 (SRID = 4269)__
```
<?xml version="1.0" encoding="UTF-8"?>
<pc:PointCloudSchema xmlns:pc="http://pointcloud.org/schemas/PC/1.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <pc:dimension>
    <pc:position>1</pc:position>
    <pc:name>X</pc:name>
    <pc:size>8</pc:size>
    <pc:interpretation>double</pc:interpretation>
  </pc:dimension>
  <pc:dimension>
    <pc:position>2</pc:position>
    <pc:name>Y</pc:name>
    <pc:size>8</pc:size>
    <pc:interpretation>double</pc:interpretation>
  </pc:dimension>
  <pc:dimension>
    <pc:position>3</pc:position>
    <pc:name>Z</pc:name>
    <pc:size>8</pc:size>
    <pc:interpretation>double</pc:interpretation>
  </pc:dimension>
  <pc:dimension>
    <pc:position>4</pc:position>
    <pc:name>yankee</pc:name>
    <pc:size>4</pc:size>
    <pc:interpretation>uint32_t</pc:interpretation>
  </pc:dimension>
  <pc:dimension>
    <pc:position>5</pc:position>
    <pc:name>zulu</pc:name>
    <pc:size>8</pc:size>
    <pc:interpretation>double</pc:interpretation>
  </pc:dimension>
  <pc:metadata>
    <Metadata name="compression">dimensional</Metadata>
  </pc:metadata>
</pc:PointCloudSchema>
```




```

# key is either the position or attribute name from the destination schema
# value is a JSON object describing the normalization to be done on the origin schema attribute
#

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

    1: {
      'expression': '$X * 100.'
    }, # simple expression. coordinates are reprojected first
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
    'zulu': {
      'expression': '($alpha ** 2) + (2 * $bravo) - $charlie'
    }
}
```
