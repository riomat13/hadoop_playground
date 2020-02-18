#!/usr/bin/env python3
#
# Processing stock data and calculate moving average with spark
# 
# Input format:
#   filename: `company_code`.us.txt
#   data format:
#       Date,Open,High,Low,Close,Volume,OpenInt
#
# In this app, use only 'data' and 'Close' columns
# Window size of moving average can be indicated by argparse
#


import os.path
import logging
import glob
from functools import partial

from pyspark import SparkContext, SparkConf

from main.settings import Config

log = logging.getLogger(__file__)


def MovingAverage(window=5):
    """Moving average calculator

    Args:
        window: int
            window size of moving average

    Returns:
        object: moving average object

    Methods:
        add(int):
            add new value
        get_average():
            return current moving average

    Usage:
        >>> ma = MovingAverage(3)
        >>> ma.add(3)
        >>> ma.get_average()
        3.0
        >>> ma.add(5)
        >>> ma.get_average()
        4.0
        >>> ma.add(7)
        >>> ma.add(9)
        >>> ma.get_average()
        7.0
    """

    arr = [0] * window
    count = 0
    size = window
    idx = 0
    total = 0.

    class base(object):
        pass

    def add(value):
        nonlocal arr
        nonlocal size
        nonlocal idx
        nonlocal total
        nonlocal count

        if count < size:
            count += 1

        total -= arr[idx]
        total += value
        arr[idx] = value
        idx += 1
        if idx == size:
            idx = 0

    def get_average():
        nonlocal total
        nonlocal count

        if count == 0:
            log.warn('No value is provided')
            return 0.

        return total / count

    base = base()
    base.add = add
    base.get_average = get_average
    return base


def get_code(path):
    basename = os.path.basename(path)
    return basename.split('.')[0]


def _process(content):
    for line in content.splitlines():
        tokens = line.split(',')

        # take year and if it is int, return
        if tokens[0][:4].isdigit():
            yield (tokens[0], float(tokens[4]))


def _calculate_average(code, content, MA):
    ma = MA()
    res = []
    for date, value in _process(content):
        ma.add(value)
        res.append((code, date, f'{ma.get_average():.3f}'))
    return res

def to_fileformat(items):
    return '\n'.join('\t'.join(item) for item in items)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Spark app for calculating moving average of stocks'
    )
    parser.add_argument('-i', '--input', type=str,
                        required=True,
                        help='input directory path')
    parser.add_argument('-o', '--output', type=str,
                        required=True,
                        help='output directory')
    parser.add_argument('-n', '--window-size', type=int,
                        default=5,
                        help='window size of moving average')

    args = parser.parse_args()

    in_dir = os.path.join(Config.ROOT_DIR, args.input)
    out_dir = os.path.join(Config.ROOT_DIR, args.output)
    log.info(f'Input directory path: {in_dir}')
    log.info(f'Output directory path: {out_dir}')

    # initial setting for process function
    window_size = args.window_size
    MA = partial(MovingAverage, window=window_size)
    calculate_average = partial(_calculate_average, MA=MA)

    log.info(f'Window size: {window_size}')

    # setup spark context
    conf = SparkConf().setAppName('stock').setMaster('local')
    sc = SparkContext(conf=conf)

    # load files
    # each file size is small so that read entire file
    raw = sc.wholeTextFiles(in_dir)

    # Stage 1:
    #   take all items and get using data
    raw = raw.map(lambda x: calculate_average(get_code(x[0]), x[1])) \
            .map(lambda x: to_fileformat(x))

    path = os.path.join(out_dir, 'spark')
    if os.path.exists(path):
        import shutil
        log.info('Removing existing directory')
        shutil.rmtree(path)
    raw.saveAsTextFile(path)
