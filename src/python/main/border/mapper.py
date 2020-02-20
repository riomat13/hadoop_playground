#!/usr/bin/env python3
"""Simple process streaming data with hadoop.

Input data form:
    Port Name,State,Port Code,Border,Date,Measure,Value
"""

import logging

from main.base.mapreduce import Mapper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)

log = logging.getLogger(__file__)


def process(line):
    """Process data by
        key:   `data,border,measure`
        value: `value`
    """
    tokens = line.split(',')
    border = tokens[3]
    date = tokens[4]
    measure = tokens[5]
    value = tokens[6]
    return ','.join((date,border,measure)), value


if __name__ == '__main__':
    mapper = Mapper(process)
    mapper.map()
