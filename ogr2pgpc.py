#!/usr/bin/python

import argparse

from ogr2pgpc.library import ogr_to_pgpointcloud

def _init_argparser():

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        '-g', '--group-by',
        action='append',
        dest='group_by',
        help="""Names of attributes to group by. Can be specified multiple 
        times. If not specified, automatic grouping is done"""
    )
    arg_parser.add_argument(
        '-l', '--layer',
        action='append',
        dest='layer',
        help="""Layer names to convert. Can be specified multiple times. If not
        specified, all layers of input file are processed"""
    )

    arg_parser.add_argument(
        '--date',
        action='append',
        dest='date',
        help="""Names of attributes to treat as Date values. Can be specified
        multiple times"""
    )
    arg_parser.add_argument(
        '--date-format',
        dest='date_format',
        help="""Date format. Formatting is based upon the Python datetime module's strptime()"""
    )
    #arg_parser.add_argument(
    #    '--time',
    #    dest='time',
    #    help="""Names of attributes to treat as Time values. Can be specified
    #    multiple times"""
    #)
    #arg_parser.add_argument(
    #    '--time-format',
    #    dest='time_format',
    #    help="""Time format. Formatting is based upon the Python datetime module's strptime()"""
    #)
    #arg_parser.add_argument(
    #    '--datetime',
    #    dest='datetime',
    #    help="""Names of attributes to treat as DateTime values. Can be
    #    specified multiple times"""
    #)
    #arg_parser.add_argument(
    #    '--datetime-format',
    #    dest='datetime_format',
    #    help="""Datetime format. Formatting is based upon the Python datetime module's strptime()"""
    #)

    # TODO
    #arg_parser.add_argument(
    #    '-tz', '--timezone',
    #    dest='timezone',
    #    help="""Timezone for time and datetime values with no timezone. If not
    #    specified, local timezone is assumed"""
    #)

    arg_parser.add_argument(
        '-p', '--pcid',
        dest='pcid',
        help='PCID of the pgPointCloud schema. This overrides the internal PCID schema creation'
    )

    arg_parser.add_argument(
        '-s', '--srid',
        dest='srid',
        help='SRID of the spatial coordinates X, Y, Z. This overrides the internal SRID estimation'
    )

    arg_parser.add_argument(
        '-t', '--tablename',
        dest='table_name',
        help="""Name of table to insert PcPatches into. If not specified, name
        of input file is used"""
    )

    arg_parser.add_argument(
        '-a', '--action',
        dest='table_action',
        default = 'create',
        help="""Action to take for the table. Possible actions are: (d)rop,
        (c)reate, (a)ppend. If not specified, (c)reate is the default"""
    )

    arg_parser.add_argument(
        '-d', '--dsn',
        dest='dsn',
        required=True,
        help='Database connection string'
    )

    arg_parser.add_argument(
        '-f', '--file',
        dest='input_file',
        required=True,
        help="OGR compatible file to be imported to pgPointCloud"
    )

    return arg_parser

def process_args(args):

    return {
        'input_file': getattr(args, 'input_file', None),
        'dsn': getattr(args, 'dsn', None),
        'group_by': getattr(args, 'group_by', []),
        'layer': getattr(args, 'layer', []),
        'srid': getattr(args, 'srid', None),
        'pcid': getattr(args, 'pcid', None),
        'table_name': getattr(args, 'table_name', None),
        'table_action': getattr(args, 'table_action', None),
    }

def run(args):

    config = process_args(args)

    ogr_to_pgpointcloud(config)

if __name__ == '__main__':
    arg_parser = _init_argparser()
    run(arg_parser.parse_args())
