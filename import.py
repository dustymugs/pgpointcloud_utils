import os
import sys
import pytz
import simplejson as json

from geojson2pgpc.library import geojson_to_pgpointcloud

if len(sys.argv) != 2:
    raise

TIMEZONE = 'US/Central'

INPUT_FILE = sys.argv[1]

config = {
    'input_file': INPUT_FILE,
    'dsn': 'dbname=solum user=postgres password=501second',
    'group_by': [],
    'ignore': ['__type'],
    'layer': [],
    'srid': None,
    'pcid': None,
    'table_name': 'public.test_pointcloud',
    'table_action': 'drop',
    'date': [],
    'time': [],
    'datetime': ['Timestamp'],
    'copy_mode': True,
    'buffer_size': 10000,
    'timezone': pytz.timezone(TIMEZONE),
}

geojson_to_pgpointcloud(config)
