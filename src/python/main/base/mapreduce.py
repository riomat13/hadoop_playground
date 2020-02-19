#!/usr/bin/env python3

import sys
import logging

import fileinput
import operator
import itertools

log = logging.getLogger(__file__)


class Mapper(object):
    def __init__(self, processor, stream=[], sep=','):
        """Base Mapper Class

        Args:
            processor: function apply for map
                processor function must return two items
            stream: a list of file paths
                read from stdin if the list is empty
            sep: separator between key and value for sending data
        """
        self.func = processor
        self.stream = stream
        self.sep = sep

    def map(self):
        for row in self:
            tokens = self.func(row)
            if len(tokens) != 2:
                log.warning(f'Invalid item for output: {tokens}')
                continue
            self.emit(*tokens)

    def emit(self, key, value):
        sys.stdout.write(f'{key}{self.sep}{value}\n')

    def __iter__(self):
        # handle both file and stdin
        with fileinput.FileInput(self.stream) as f:
            try:
                for content in f:
                    for row in content.splitlines():
                        yield row
            except Exception as e:
                log.warning(f'Error occured in {repr(f)}: {e}')


class Reducer(object):
    def __init__(self, processor, sep=','):
        """Base Reducer Class

        This will read items from stdin processed by mapper

        Args:
            processor: function apply for map
                processor function must return two items
            sep: separator between key and value for sending data
        """
        self.func = processor
        self.sep = sep

    def reduce(self):
        for key, group in itertools.groupby(self, operator.itemgetter(0)):
            item = self.func(group)
            self.emit(key, item)

    def emit(self, key, value):
        sys.stdout.write(f'{key}{self.sep}{value}\n')

    def __iter__(self):
        # read only from stdin
        for content in sys.stdin:
            try:
                for row in content.splitlines():
                    yield row.strip().split(self.sep)
            except Exception as e:
                log.warning(f'Error occured in {content}: {e}')
