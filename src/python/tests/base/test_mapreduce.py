#!/usr/bin/env python3

import unittest
from unittest.mock import patch, MagicMock
import test.support

import io

from main.base.mapreduce import Mapper, Reducer


@patch('main.base.mapreduce.sys.stdout', new_callable=io.StringIO)
class MapperTest(unittest.TestCase):

    def test_emit_data_from_stdin(self, stdout):
        try:
            target = ['1,2', '3,4', '5,6']

            mapper = Mapper(lambda x: x.split(','))
            emitted = []

            with test.support.captured_stdin() as stdin:
                for line in target:
                    stdin.write(line + '\n')

                stdin.seek(0)

                mapper.map()

            # items output to stdout is stored in StringIO
            received = stdout.getvalue()
            items = []
            for item in received.splitlines():
                items.append(item)

            self.assertEqual(items, target)

        finally:
            stdout.close()

    def test_handle_invalid_input(self, stdout):
        try:
            target = ['1,2', '3,4', '5,6']
            invalid = ['1,2,3', '4,5,6']

            mapper = Mapper(lambda x: x.split(','))
            emitted = []

            with test.support.captured_stdin() as stdin:
                for line in target + invalid:
                    stdin.write(line + '\n')

                stdin.seek(0)

                # if invalid data is found, send log as warning
                # and skip the data
                with self.assertLogs(level='WARNING'):
                    mapper.map()

            # items output to stdout is stored in StringIO
            received = stdout.getvalue()
            items = []
            for item in received.splitlines():
                items.append(item)

        finally:
            stdout.close()


@patch('main.base.mapreduce.sys.stdout', new_callable=io.StringIO)
class ReducerTest(unittest.TestCase):

    def test_read_output_from_mapper_and_reduce(self, stdout):
        # simple suming task
        def reduce_func(group):
            total = 0
            for item in group:
                total += int(item[1])
            return total

        reducer = Reducer(reduce_func)

        try:
            # data must be sorted by key
            contents = ['a,1', 'a,3', 'b,2', 'b,1']

            # setup target data after reduced
            target = {}
            for item in contents:
                k, v = item.split(',')
                target[k] = target.get(k, 0) + int(v)

            with test.support.captured_stdin() as stdin:
                for line in contents:
                    stdin.write(line + '\n')

                stdin.seek(0)

                reducer.reduce()

            # items output to stdout is stored in StringIO
            received = stdout.getvalue()
            items = []
            for item in received.splitlines():
                items.append(item)

            self.assertEqual(len(items), len(target))

            for item in items:
                k, v = item.strip().split(',')
                self.assertEqual(int(v), target[k])

        finally:
            stdout.close()


if __name__ == '__main__':
    unittest.main()
