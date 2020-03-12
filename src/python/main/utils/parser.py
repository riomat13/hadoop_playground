#!/usr/bin/env python3

import argparse


def get_parser(description=''):
    """Build base parser

    Initial argumets are:
        -i, --input:  Input directory path containing dataset
        -o, --output: Output directory path
        --parquet:    Set this to save as Parquet file
        --from-local: Set this to read data in local file system
        --to-hdfs:    Set this to save data in hdfs
        --gzip:       Set this to compress data by gzip
    """
    parser = argparse.ArgumentParser(
            description=description,
            formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('-i', '--input', type=str,
                        required=True,
                        help='Input directory path containing dataset')
    parser.add_argument('-o', '--output', type=str,
                        required=True,
                        help='Output directory path')

    parser.add_argument('--parquet', action='store_true',
                        help='Set this to save as Parquet file')
    parser.add_argument('--from-local', action='store_true',
                        help='Set this to read data in local file system')
    parser.add_argument('--to-hdfs', action='store_true',
                        help='Set this to save data in hdfs')
    parser.add_argument('--sequenceFile', action='store_true',
                        help='Set this to save as SequenceFile\n'
                             'This works only when to_hdfs is set')
    parser.add_argument('--gzip', action='store_true',
                        help='Set this to compress data by gzip')

    return parser
