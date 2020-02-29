#!/usr/bin/env python3

import unittest
from unittest.mock import patch

import warnings

import copy
from datetime import datetime
from decimal import Decimal
from itertools import combinations

from pyspark import SparkContext, SparkConf

from main.netflix.app import (
    flatten_user_data,
    count_value_items,
    add_num_rates_to_user_data,
    build_movie_pairs,
    calc_pair_parameters,
    calc_similarity,
)


sc = None


def setUpModule():
    global sc

    # supress warning from third-party library per each test
    warnings.simplefilter('ignore', ResourceWarning)
    conf = SparkConf() \
        .setAppName('test-movie-recommendation') \
        .setMaster('local[2]')
    sc = SparkContext.getOrCreate(conf=conf)
    sc.setLogLevel('WARN')

def tearDownModule():
    sc.stop()


class TestProcessData(unittest.TestCase):

    def test_flatten_userdata(self):
        """Stage 1:
        Test split items in value and
        flatten with (key, value) combinations."""
        data = sc.parallelize([
            ['000100-r-00000', '10,1,2000-01-01\n20,2,2000-02-02'],
            ['000101-r-00000', '15,3,2000-03-03\n25,4,2000-04-04'],
        ])
        result = flatten_user_data(data).collect()

        self.assertEqual(len(result), 4)

        # check if data is converted into int
        for id_, data in result:
            self.assertTrue(isinstance(id_, int))
            # output should be [movie_id, rating, timestamp]
            self.assertEqual(len(data), 3)
            self.assertTrue(all([isinstance(ele, int) for ele in data]))

    def test_count_value_items(self):
        """Stage 3:
        Test count items in value.
        Can define how to split between key-value
        and between each item in value."""
        items = [
            '1111\t101,102,103',
            '1112\t101,102,104,105',
            '1113\t103,105,106',
        ]

        data = sc.parallelize(items)

        # setup target data:
        #   [(movie1, num_of_users), (movie2, num_of_users), ...]
        target = [item.split('\t') for item in items]
        target = set(list(map(lambda x: (int(x[0]), len(x[1].split(','))), target)))

        result = count_value_items(data).collect()

        for data in result:
            if data not in target:
                self.fail(f'not found target data: {data}')

            target.remove(data)

        # check all data is collected
        self.assertEqual(len(target), 0)

    def test_add_item_to_user_data(self):
        """Stage 4:
        Test adding(join) item from other rdd to main rdd by item in value."""
        users = [
            [10, [1, 5, 1000]],
            [11, [2, 2, 1001]],
            [12, [3, 3, 1002]],
        ]

        movies = [
            (1, 3),
            (2, 5),
            (3, 7),
        ]

        target = copy.deepcopy(users)
        for i, movie in enumerate(movies):
            target[i][1].append(movie[1])

        u = sc.parallelize(users)
        m = sc.parallelize(movies)
        result = add_num_rates_to_user_data(u, m).collect()

        for i, res in enumerate(result):
            self.assertEqual(res[0], target[i][0])
            self.assertTrue(all(r == t for r, t in zip(res[1], target[i][1])))

    def test_build_movie_pairs(self):
        """Test all combination of movies for each user."""
        timestamp = int(datetime(2000, 10, 1).timestamp()) // 3600

        items = [
            (10,1,10000),
            (20,2,20000),
            (30,1,30000),
            (40,4,40000),
        ]

        pairs = combinations(items, 2)

        data = sc.parallelize([
            (100, item) for item in items
        ])

        target = [(100, pair) for pair in pairs]

        result = build_movie_pairs(data).collect()

        self.assertEqual(len(result), len(target))

        for key, pair in result:
            self.assertEqual(len(pair), 2)

        target = set(target)
        result = set(result)

        for ele in result:
            if ele not in target:
                self.fail('not found target data')

            target.remove(ele)

        # check all data is collected
        self.assertEqual(len(target), 0)

    def test_calculate_movie_parameters(self):
        """Test adding parameters to compute similarities."""
        pairs = [
            (100, ((10, 1, 10000, 3), (20, 2, 20000, 4))),
            (100, ((10, 1, 10000, 3), (30, 1, 30000, 5))),
            (100, ((10, 1, 10000, 3), (40, 4, 40000, 6))),
            (100, ((20, 2, 20000, 4), (30, 1, 30000, 5))),
            (100, ((20, 2, 20000, 4), (40, 4, 40000, 6))),
            (100, ((30, 1, 30000, 5), (40, 4, 40000, 6)))
        ]

        max_raters = max([max(a[1][0][3], a[1][1][3]) for a in pairs])

        targets = [
            ((pair[1][0][0], pair[1][1][0]),
             (Decimal(pair[1][0][1]) / 5, Decimal(pair[1][0][3]) / max_raters,
              Decimal(pair[1][1][1]) / 5, Decimal(pair[1][1][3]) / max_raters))
            for pair in pairs
        ]

        data = sc.parallelize(pairs)
        result = calc_pair_parameters(data, max_raters=max_raters) \
            .collect()

        # check the size of output data
        self.assertEqual(len(result), len(targets))
        # check output has key, value combination
        self.assertEqual(len(result[0]), 2)
        # check key size
        self.assertEqual(len(result[0][0]), len(targets[0][0]))
        # check value size
        self.assertEqual(len(result[0][1]), len(targets[0][1]))
        # check processed items
        for res, target in zip(result, targets):
            self.assertTrue(all(r == t for r, t in zip(res[0], target[0])))
            self.assertTrue(all(r == t for r, t in zip(res[1], target[1])))

    def test_calculate_similarities(self):
        """Test calculating similarity and return values are valid."""
        data = [
            [10, [0.2, 0.3, 0.1, 0.5]],
            [20, [0.2, 0.3, 0.1, 0.5]],
            [20, [0.2, 0.3, 0.1, 0.5]],
        ]

        data = [[k, list(map(Decimal, v))] for k, v in data]

        data = sc.parallelize(data)


        result = calc_similarity(data).collect()

        # TODO: add Pearson and Jaccard
        self.assertEqual(len(result[0][1]), 1)
        self.assertTrue(all(isinstance(r, Decimal) for r in result[0][1]))
        self.assertTrue(all(r > 0.0 for r in result[0][1]))



if __name__ == '__main__':
    unittest.main()
