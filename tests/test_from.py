#!/usr/bin/env python

import os
import csv
import os.path
import shutil
import tempfile
import pandas as pd
import pyarrow.parquet
from unittest import TestCase, main
from airflow_xlsx.from_xlsx_operator import FromXLSXOperator

TEST_DATA = [
    ['One', '2021-09-07 00:00:00', '10', '1', '10', '0.0001'],
    ['Two', '2011-09-08 00:00:00', '20', '0', '30', '0.0002'],
    ['à§æ', '2011-09-09 00:00:00', '30', '1', '60', '0.0004'],
]


class TestUtils(TestCase):
    def setUp(self):
        self.root_dir = os.path.dirname(os.path.realpath(__file__))
        self.target_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.target_dir)

    def test_xlsx_to_csv(self):
        source = os.path.join(self.root_dir, 'test.xlsx')
        target = os.path.join(self.target_dir, 'test.xlsx.csv')
        so = FromXLSXOperator(
            task_id='test',
            source=source,
            target=target,
            csv_delimiter='|',
            file_format='csv',
        )
        so.execute({})
        with open(target, 'r') as f:
            reader = csv.reader(f, delimiter='|')
            for i, row in enumerate(reader):
                if i > 1:
                    self.assertEqual(row, TEST_DATA[i - 1])

    def test_xls_to_csv(self):
        source = os.path.join(self.root_dir, 'test.xls')
        target = os.path.join(self.target_dir, 'test.xls.csv')
        so = FromXLSXOperator(
            task_id='test',
            source=source,
            target=target,
            limit=10,
            csv_delimiter='|',
            file_format='csv',
        )
        so.execute({})
        with open(target, 'r') as f:
            reader = csv.reader(f, delimiter='|')
            for i, row in enumerate(reader):
                if i >= 1:
                    self.assertEqual(row, TEST_DATA[i - 1])

    def test_xls_to_parquet(self):
        source = os.path.join(self.root_dir, 'test.xls')
        target = os.path.join(self.target_dir, 'test.xls.csv')
        so = FromXLSXOperator(
            task_id='test',
            source=source,
            target=target,
            file_format='parquet',
        )
        so.execute({})
        p = pyarrow.parquet.read_table(target).to_pandas()
        print(p)
        self.assertEqual(len(p), 3)
        self.assertTrue(all(p['col3number'] == [10, 20, 30]))
        self.assertTrue(all(p['col4numformula'] == [10, 30, 60]))


if __name__ == '__main__':
    main()
