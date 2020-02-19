#!/usr/bin/env python3

import unittest
from unittest.mock import patch, MagicMock
import test.support

import io

from main.base.mapreduce import Mapper


@patch('main.base.mapreduce.sys.stdout', new_callable=io.StringIO)
class MapperTest(unittest.TestCase):

    def test_emit_data_from_stdin(self, stdout):
        try:
            target = ['1,2', '3,4', '5,6']

            self.mapper = Mapper(lambda x: x.split(','))
            emitted = []

            with test.support.captured_stdin() as stdin:
                for line in target:
                    stdin.write(line + '\n')

                stdin.seek(0)

                self.mapper.map()

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

            self.mapper = Mapper(lambda x: x.split(','))
            emitted = []

            with test.support.captured_stdin() as stdin:
                for line in target + invalid:
                    stdin.write(line + '\n')

                stdin.seek(0)

                # if invalid data is found, send log as warning
                # and skip the data
                with self.assertLogs(level='WARNING'):
                    self.mapper.map()

            # items output to stdout is stored in StringIO
            received = stdout.getvalue()
            items = []
            for item in received.splitlines():
                items.append(item)

        finally:
            stdout.close()


if __name__ == '__main__':
    unittest.main()
