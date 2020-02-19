#!/usr/bin/env python3

import unittest

from main.base.obj import Date, DateFactory


class DateTest(unittest.TestCase):
    def test_create_by_format(self):
        year = 2010
        month = 6
        data = f'{month:02d}/01/{year} 12:00:00 AM'
        d = Date.from_string(data, format='%m/%d/%Y %H:%M:%S %p')

        self.assertEqual(d.year, year)
        self.assertEqual(d.month, month)
        self.assertEqual(d._day, 1)

    def test_sort_by_year(self):
        month = 1
        d1 = Date(2010, month)
        d2 = Date(2005, month)
        d3 = Date(2008, month)
        d4 = Date(2009, month)
        dates = [d1, d2, d3, d4]

        dates.sort()

        prev = 0
        for date in dates:
            self.assertGreater(date.year, prev)
            prev = date.year

    def test_sort_by_month(self):
        year = 2010
        d1 = Date(year, 10)
        d2 = Date(year, 3)
        d3 = Date(year, 4)
        d4 = Date(year, 1)
        dates = [d1, d2, d3, d4]

        dates.sort()

        prev = 0
        for date in dates:
            self.assertGreater(date.month, prev)
            prev = date.month


class DateFactoryTest(unittest.TestCase):

    def setUp(self):
        self.factory = DateFactory()

    def test_generate_from_strings(self):
        items = [
            '10/01/2011 12:00:00 AM',
            '12/01/2009 12:00:00 AM',
            '03/01/2015 12:00:00 AM',
        ]

        res = []
        for d in self.factory.generate_all(items):
            self.assertEqual(d.__class__, Date)
            res.append(d)

        self.assertEqual(len(res), len(items))
