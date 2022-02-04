#!/usr/bin/env python

import os
import json
import csv
import os.path
import shutil
import tempfile
from unittest import TestCase, main
import pandas as pd
import pyarrow.parquet
from xlsx_provider.operators.from_xlsx_query_operator import FromXLSXQueryOperator
from xlsx_provider.operators.from_xlsx_operator import FromXLSXOperator

TEST_DATA_TYPED = [
    ['code', 'line', 'value', 'score'],
    ['A1', 1, 4844404, 0.1],
    ['A2', 2, 5319973, 0.2],
    ['A3', 3, 3145626, 0.3],
    ['B1', 4, 5350372, 0.4],
    ['B2', 5, 5163642, 0.5],
    ['B3', 6, 4931636, 0.6],
    ['C1', 7, 2926709, 0.7],
    ['C2', 8, 5167375, 0.8],
    ['C3', 9, 4783424, 0.9],
    ['C4', 10, 1, 1]
]

TEST_DATA = [
    ['code', 'line', 'value', 'score'],
    ['A1', '1', '4844404', '0.1'],
    ['A2', '2', '5319973', '0.2'],
    ['A3', '3', '3145626', '0.3'],
    ['B1', '4', '5350372', '0.4'],
    ['B2', '5', '5163642', '0.5'],
    ['B3', '6', '4931636', '0.6'],
    ['C1', '7', '2926709', '0.7'],
    ['C2', '8', '5167375', '0.8'],
    ['C3', '9', '4783424', '0.9'],
    ['C4', '10', '1', '1']
]

QUERY = """
    select
        a as code,
        b as line,
        c as value,
        d as score
    from
        xls
    where
        _index > 1
        and a <> ''
"""

class TestFrom(TestCase):
    def setUp(self):
        self.root_dir = os.path.dirname(os.path.realpath(__file__))
        self.target_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.target_dir)

    def test_query_to_csv(self):
        source = os.path.join(self.root_dir, 'types.xlsx')
        target = os.path.join(self.target_dir, 'types.xlsx.csv')
        so = FromXLSXQueryOperator(
            task_id='test',
            source=source,
            target=target,
            csv_delimiter='|',
            file_format='csv',
            query=QUERY,
        )
        so.execute({})
        with open(target, 'r') as f:
            reader = csv.reader(f, delimiter='|')
            for i, row in enumerate(reader):
                if i >= 1:
                    self.assertEqual(row, TEST_DATA[i])

    def test_query_to_parquet(self):
        source = os.path.join(self.root_dir, 'types.xlsx')
        target = os.path.join(self.target_dir, 'types.xlsx.parquet')
        so = FromXLSXQueryOperator(
            task_id='test',
            source=source,
            target=target,
            file_format='parquet',
            query=QUERY,
        )
        so.execute({})
        df = pyarrow.parquet.read_table(target).to_pandas()
        self.assertEqual(len(df), 10)
        for col, name in enumerate(df.keys()):
            data = [x[col] for i, x in enumerate(TEST_DATA_TYPED) if i > 0]
            series = pd.Series(data, dtype=df[name].dtype)
            self.assertTrue(df[name].equals(series))

    def test_xlsx_to_csv(self):
        source = os.path.join(self.root_dir, 'types.xlsx')
        target = os.path.join(self.target_dir, 'types.xlsx.csv')
        so = FromXLSXOperator(
            task_id='types',
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
                    self.assertEqual(row, TEST_DATA[i])

    def test_xls_to_csv(self):
        source = os.path.join(self.root_dir, 'types.xls')
        target = os.path.join(self.target_dir, 'types.xls.csv')
        so = FromXLSXOperator(
            task_id='types',
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
                    self.assertEqual(row, TEST_DATA[i])

    def test_xlsx_to_parquet(self):
        source = os.path.join(self.root_dir, 'types.xlsx')
        target = os.path.join(self.target_dir, 'types.xlsx.parquet')
        so = FromXLSXOperator(
            task_id='test',
            source=source,
            target=target,
            file_format='parquet',
        )
        so.execute({})
        df = pyarrow.parquet.read_table(target).to_pandas()
        self.assertEqual(len(df), 10)
        for col, name in enumerate(df.keys()):
            data = [x[col] for i, x in enumerate(TEST_DATA_TYPED) if i > 0]
            series = pd.Series(data, dtype=df[name].dtype)
            self.assertTrue(df[name].equals(series))


    def test_xls_to_parquet(self):
        source = os.path.join(self.root_dir, 'types.xls')
        target = os.path.join(self.target_dir, 'types.xls.parquet')
        so = FromXLSXOperator(
            task_id='test',
            source=source,
            target=target,
            file_format='parquet',
        )
        so.execute({})
        df = pyarrow.parquet.read_table(target).to_pandas()
        self.assertEqual(len(df), 10)
        for col, name in enumerate(df.keys()):
            data = [x[col] for i, x in enumerate(TEST_DATA_TYPED) if i > 0]
            series = pd.Series(data, dtype=df[name].dtype)
            self.assertTrue(df[name].equals(series))



if __name__ == '__main__':
    main()
