# geojson2pgpc

Import GeoJSON files as pgPointCloud patches. This utility is a stripped version of ogr2pgpc for performance reasons.

## Requirements

* psycopg2
* shapely
* dateutil
* pytz
* tzlocal

## Usage

* -h, --help
 show this help message and exit
* -y, --copy
 Use COPY statements instead of INSERT statements
* -b BUFFER_SIZE, --buffer BUFFER_SIZE
 Flush to database every X records
* -g GROUP_BY, --group-by GROUP_BY
Names of attributes to group by. Can be specified
multiple times. If not specified, automatic grouping
is done
* -i IGNORE, --ignore IGNORE
Names of attributes to ignore. Can be specified
multiple times. If not specified, all attributes are
considered
* -l LAYER, --layer LAYER
Layer names to convert. Can be specified multiple
times. If not specified, all layers of input file are
processed
* --date DATE
Names of attributes to treat as Date values. Can be
specified multiple times
* --time TIME
Names of attributes to treat as Time values. Can be
specified multiple times
* --datetime DATETIME
Names of attributes to treat as DateTime values. Can
be specified multiple times
* -tz TIMEZONE, --timezone TIMEZONE
Timezone for time and datetime values with no
timezone. If not specified, local timezone is assumed
* -p PCID, --pcid PCID
PCID of the pgPointCloud schema. This overrides the
internal PCID schema creation
* -s SRID, --srid SRID
SRID of the spatial coordinates X, Y, Z. This
overrides the internal SRID estimation
* -t TABLE_NAME, --tablename TABLE_NAME
Name of table to insert PcPatches into. If not
specified, name of input file is used
* -a {create,append,drop}, --action {create,append,drop}
Action to take for the table. create is the default action
* -d DSN, --dsn DSN
Database connection string
* -f INPUT_FILE, --file INPUT_FILE
GeoJSON file to be imported to pgPointCloud

