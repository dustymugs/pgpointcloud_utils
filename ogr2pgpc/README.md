# geojson2pgpc

Import OGR-compatible datasets as pgPointCloud patches

## Requirements

* psycopg2
* ogr
* shapely
* dateutils
* pytz
* tzlocal

## Usage

* __-h, --help__

  show this help message and exit

* __-y, --copy__

  Use COPY statements instead of INSERT statements

* __-b BUFFER_SIZE, --buffer BUFFER_SIZE__

  Flush to database every X records

* __-m METADATA, --metadata METADATA__

  Metadata outside the OGR file to include with generated PCPatches

* __-g GROUP_BY, --group-by GROUP_BY__

  Names of attributes to group by. Can be specified multiple times. If not specified, automatic grouping is done

* __-i IGNORE, --ignore IGNORE__

  Names of attributes to ignore. Can be specified multiple times. If not specified, all attributes are considered

* __-l LAYER, --layer LAYER__

  Layer names to convert. Can be specified multiple times. If not specified, all layers of input file are processed

* __--date DATE__

  Names of attributes to treat as Date values. Can be specified multiple times

* __--time TIME__

  Names of attributes to treat as Time values. Can be specified multiple times

* __--datetime DATETIME__

  Names of attributes to treat as DateTime values. Can be specified multiple times

* __-tz TIMEZONE, --timezone TIMEZONE__

  Timezone for time and datetime values with no timezone. If not specified, local timezone is assumed

* __-p PCID, --pcid PCID__

  PCID of the pgPointCloud schema. This overrides the internal PCID schema search and creation

* __-s SRID, --srid SRID__

  SRID of the spatial coordinates X, Y, Z. This overrides the internal SRID estimation

* __-t TABLE_NAME, --tablename TABLE_NAME__

  Name of table to insert PcPatches into. If not specified, name of input file is used

* __-a {create,append,drop}, --action {create,append,drop}__

  Action to take for the table. _create_ is the default action

* __-d DSN, --dsn DSN__

  Database connection string

* __-f INPUT_FILE, --file INPUT_FILE__

  OGR compatible file to be imported to pgPointCloud

As there is no way to directly store _DATE_, _TIME_ and _DATETIME_ values in a supported pgPointCloud datatype, these values are converted to the number of seconds UTC from UNIX epoch. The converted values are stored as _double_ to capture milliseconds, if any.

The PcPatch table created or appended to looks like:

```
CREATE TABLE pcpatch_table (
    id BIGSERIAL PRIMARY KEY,
    pa PCPATCH,
    layer_name TEXT,
    file_name TEXT,
    group_by JSON,
    metadata JSON
)
```
