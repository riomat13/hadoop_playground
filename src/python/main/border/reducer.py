#!/usr/bin/env python3

"""Simple reduce process for hadoop MapReduce operation.

Input data form:
    Date,Border,Measure,Value
"""

import sys
import itertools
import operator
import heapq
import logging

from main.base.mapreduce import Reducer
from main.base.obj import Date

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)


log = logging.getLogger(__file__)


def sum_in_group(group):
    """Process data for first reduce task.

    Input is values grouped by key.
    """
    total = 0
    try:
        for k, v in group:
            total += int(v)
    except Exception as e:
        log.error(f'{sum_in_group.__name__}: Error occured in "{g}": {e}')
    return total


class GroupingReducer(Reducer):

    def reduce(self):
        for key, group in itertools.groupby(self, operator.itemgetter(0)):
            item = self.func(group)
            yield self.emit(key, item)

    def emit(self, key, value):
        # output items without streaming
        return (key, value)

    def __iter__(self):
        # read only from stdin
        for row in sys.stdin:
            try:
                *key, value = row.strip().split(self.sep)
                yield ','.join(key), value
            except Exception as e:
                log.error(f'{self.__class__.__name__}: Error occured in "{content}": {e}')


def secondary_mapper(items):
    try:
        for key, value in items:
            date, border, measure = key.split(',')
            yield (date, f'{border},{measure},{value}')
    except Exception as e:
        log.error(f'{secondary_mapper.__name__}: Error occured in "{items}": {e}')


def secondary_sort(group):
    for item in group:
        log.info(f'processing data: {item}')
        border, measure, value = item.split(',')
        value = int(value)
        # sort by value in descending order
        heapq.heappush(items, (-value, border, measure))
    while items:
        value, border, measure = heapq.heappop(items)
        yield f'{border},{measure},{-value}'


class SecondarySort(Reducer):

    def __init__(self):
        self.sep = ','

    def reduce(self, items):
        sorted_items = []
        try:
            for k, v in items:
                border, measure, value = v.split(',')
                value = int(value)
                # sort by value in descending order
                date = Date.from_string(k, '%m/%d/%Y %H:%M:%S %p')
                heapq.heappush(sorted_items, (date, -value, border, measure))

            while sorted_items:
                key, value, border, measure = heapq.heappop(sorted_items)
                key = f'{key.month:02d}/01/{key.year:04d} 12:00:00 AM'
                self.emit(key, f'{border},{measure},{-value}')

        except Exception as e:
            log.error(f'{self.__class__.__name__}: Error occured in "{items}": {e}')


if __name__ == '__main__':
    reducer1 = GroupingReducer(sum_in_group)
    items = reducer1.reduce()

    # regrouping by date
    items = secondary_mapper(items)

    sorter = SecondarySort()
    sorter.reduce(items)
