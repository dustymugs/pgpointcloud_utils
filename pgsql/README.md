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

_**WARNING** Order is not guaranteed in JSON_

_**WARNING** Though the same key can be provided in JSON, the last key will overwrite all prior instances of that key_


* map source name to destination name

```
{
  ...
  "dest_key": "origin_key",
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
  "dest_key": 5
  ...
}
```

* map source name to destination position

```
{
  ...
  5: "origin_key"
  ...
}
```

* map a constant to destination

```
{
  ...
  5: {
    "value": 65535
  },
  "dest_key": {
    "value": 65535
  }
  ...
}
```

* map an expression to destination

```
{
  ...
  5: {
    "expression": "$origin_key ** 2"
  },
  "dest_key": {
    "expression": "1. / $origin_key"
  }
  ...
}
```

* map _null_ to destination

  This applies the destination position or key to the origin. Position 5 of origin maps to position 5 of destination. Key "dest_key" of origin maps to key "dest_key" of destination.

```
{
  ...
  5: null,
  "dest_key": null
}
```

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

__Transform from PCID 1 to 10__
```
SELECT PC_Transform(pt, 10, '{
    1: 1,
    "Y", 2,
    3: "Z",
    "charlie": null,
    "alpha": null,
    "bravo": null
}'::json)
```

__Transform from PCID 1 to 10__
```
SELECT PC_Transform(pt, 10, '{
    1: {
      "expression": "$X * 100."
    },
    "Y", 2,
    3: "Z",
    "alpha": null,
    "bravo": null
    "charlie": null,
}'::json)
```

__Transform from PCID 1 to 20__
```
SELECT PC_Transform(pt, 10, '{
    "X": null,
    "Y": null,
    "Z": null,
    "yankee": {
      "value": 9999999
    },
    "zulu": {
      "expression": "($alpha ** 2) + (2 * $bravo) - $charlie"
    }
}'::json)
```
