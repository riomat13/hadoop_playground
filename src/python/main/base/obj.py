#!/usr/bin/env python3

from abc import ABC, abstractmethod

from datetime import datetime


class _Comparable(ABC):

    @abstractmethod
    def compare_to(self, other):
        return

    def __eq__(self, other):
        return self is other or self.compare_to(other) == 0

    def __ne__(self, other):
        return self.compare_to(other) != 0

    def __lt__(self, other):
        return self.compare_to(other) < 0

    def __le__(self, other):
        return self.compare_to(other) <= 0

    def __gt__(self, other):
        return self.compare_to(other) > 0

    def __ge__(self, other):
        return self.compare_to(other) >= 0


class Date(_Comparable):
    def __init__(self, year: int, month: int):
        if not (1970 < year < 2025 and 0 < month < 13):
            raise ValueError('Invalid year and month')

        self._year = year
        self._month = month
        # TODO: handle day
        self._day = 1

    def compare_to(self, other):
        if type(self) is not type(other):
            raise ValueError("Comparing different class object")

        comp = self._year - other._year
        if (comp == 0):
            comp = self._month - other._month
        # TODO: add day

        return comp

    @property
    def year(self):
        return self._year

    @property
    def month(self):
        return self._month

    def __str__(self):
        return f'{self._year}-{self._month:02d}-{self._day:02d}'

    def __repr__(self):
        return f'_Date(year={self._year}, month={self._month})'

    @staticmethod
    def from_datetime(dttm):
        """Instantiate object by datetime/date object."""
        return Date(dttm.year, dttm.month)

    @staticmethod
    def from_string(string, format):
        dttm = datetime.strptime(string, format)
        return Date(dttm.year, dttm.month)


class DateFactory(object):
    def __init__(self, format='%m/%d/%Y %H:%M:%S %p'):
        self.format = format

    def generate(self, line):
        """Generate _Date object based on given format."""
        return Date.from_string(line, self.format)

    def generate_all(self, lines):
        """Generate _Date object by iterable."""
        for line in lines:
            yield self.generate(line)
