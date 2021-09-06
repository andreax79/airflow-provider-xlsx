#!/usr/bin/env python

import os
import csv
import json
import os.path
import shutil
import tempfile
import pyarrow.parquet
from unittest import TestCase, main
from xlsx_provider.operators.to_xlsx_operator import ToXLSXOperator
from xlsx_provider.operators.from_xlsx_operator import FromXLSXOperator

TEST_DATA = [
    ['One', '2021-09-07 00:00:00', '10', '1', '10', '0.0001'],
    ['Two', '2011-09-08 00:00:00', '20', '0', '30', '0.0002'],
    ['à§æ', '2011-09-09 00:00:00', '30', '1', '60', '0.0004'],
]

TEST_DATA_JSON = [
    {
        'col1label': 'One',
        'col2date': '2021-09-07 00:00:00',
        'col3number': 10,
        'col4boolean': 1,
        'col4numformula': 10,
        'col5num': 0.0001,
    },
    {
        'col1label': 'Two',
        'col2date': '2011-09-08 00:00:00',
        'col3number': 20,
        'col4boolean': 0,
        'col4numformula': 30,
        'col5num': 0.0002,
    },
    {
        'col1label': 'à§æ',
        'col2date': '2011-09-09 00:00:00',
        'col3number': 30,
        'col4boolean': 1,
        'col4numformula': 60,
        'col5num': 0.0004,
    },
]


class TestTo(TestCase):
    def setUp(self):
        self.root_dir = os.path.dirname(os.path.realpath(__file__))
        self.target_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.target_dir)

    def test_csv_xlsx(self):
        source = os.path.join(self.root_dir, 'test.xlsx.csv')
        target = os.path.join(self.target_dir, 'test.xlsx')
        target_check = os.path.join(self.target_dir, 'test.xlsx.csv')
        so = ToXLSXOperator(
            task_id='test',
            source=source,
            target=target,
            csv_delimiter='|',
        )
        so.execute({})
        so = FromXLSXOperator(
            task_id='test',
            source=target,
            target=target_check,
            limit=10,
            csv_delimiter='|',
            file_format='csv',
        )
        so.execute({})
        lines = 0
        with open(target_check, 'r') as f:
            reader = csv.reader(f, delimiter='|')
            for i, row in enumerate(reader):
                if i > 0:
                    lines += 1
                    self.assertEqual(row, TEST_DATA[i - 1])
        self.assertEqual(lines, len(TEST_DATA))

    def test_parquet_xlsx(self):
        source = os.path.join(self.root_dir, 'test.xls.parquet')
        target = os.path.join(self.target_dir, 'test.xlsx')
        target_check = os.path.join(self.target_dir, 'test.xlsx.csv')
        so = ToXLSXOperator(
            task_id='test',
            source=source,
            target=target,
        )
        so.execute({})
        so = FromXLSXOperator(
            task_id='test',
            source=target,
            target=target_check,
            limit=10,
            csv_delimiter='|',
            file_format='csv',
        )
        so.execute({})
        lines = 0
        with open(target_check, 'r') as f:
            reader = csv.reader(f, delimiter='|')
            for i, row in enumerate(reader):
                if i > 0:
                    lines += 1
                    self.assertEqual(row, TEST_DATA[i - 1])
        self.assertEqual(lines, len(TEST_DATA))

    def test_json_xlsx(self):
        source = os.path.join(self.root_dir, 'test.xls.json')
        target = os.path.join(self.target_dir, 'test.xlsx')
        target_check = os.path.join(self.target_dir, 'test.xlsx.csv')
        so = ToXLSXOperator(
            task_id='test',
            source=source,
            target=target,
        )
        so.execute({})
        so = FromXLSXOperator(
            task_id='test',
            source=target,
            target=target_check,
            limit=10,
            csv_delimiter='|',
            file_format='csv',
        )
        so.execute({})
        lines = 0
        with open(target_check, 'r') as f:
            reader = csv.reader(f, delimiter='|')
            for i, row in enumerate(reader):
                if i > 0:
                    lines += 1
                    self.assertEqual(row, TEST_DATA[i - 1])
        self.assertEqual(lines, len(TEST_DATA))

    def test_jsonl_xlsx(self):
        source = os.path.join(self.root_dir, 'test.xls.jsonl')
        target = os.path.join(self.target_dir, 'test.xlsx')
        target_check = os.path.join(self.target_dir, 'test.xlsx.csv')
        so = ToXLSXOperator(
            task_id='test',
            source=source,
            target=target,
        )
        so.execute({})
        so = FromXLSXOperator(
            task_id='test',
            source=target,
            target=target_check,
            limit=10,
            csv_delimiter='|',
            file_format='csv',
        )
        so.execute({})
        lines = 0
        with open(target_check, 'r') as f:
            reader = csv.reader(f, delimiter='|')
            for i, row in enumerate(reader):
                if i > 0:
                    lines += 1
                    self.assertEqual(row, TEST_DATA[i - 1])
        self.assertEqual(lines, len(TEST_DATA))

    def test_xls_xlsx(self):
        source = os.path.join(self.root_dir, 'test.xls')
        target = os.path.join(self.target_dir, 'test.xlsx')
        target_check = os.path.join(self.target_dir, 'test.xlsx.csv')
        so = ToXLSXOperator(
            task_id='test',
            source=source,
            target=target,
        )
        so.execute({})
        so = FromXLSXOperator(
            task_id='test',
            source=target,
            target=target_check,
            limit=10,
            csv_delimiter='|',
            file_format='csv',
        )
        so.execute({})
        lines = 0
        with open(target_check, 'r') as f:
            reader = csv.reader(f, delimiter='|')
            for i, row in enumerate(reader):
                if i > 0:
                    lines += 1
                    self.assertEqual(row, TEST_DATA[i - 1])
        self.assertEqual(lines, len(TEST_DATA))

    def test_xlsx_xlsx(self):
        source = os.path.join(self.root_dir, 'test.xlsx')
        target = os.path.join(self.target_dir, 'test.xlsx')
        target_check = os.path.join(self.target_dir, 'test.xlsx.csv')
        so = ToXLSXOperator(
            task_id='test',
            source=source,
            target=target,
        )
        so.execute({})
        so = FromXLSXOperator(
            task_id='test',
            source=target,
            target=target_check,
            limit=10,
            csv_delimiter='|',
            file_format='csv',
        )
        so.execute({})
        lines = 0
        with open(target_check, 'r') as f:
            reader = csv.reader(f, delimiter='|')
            for i, row in enumerate(reader):
                if i > 0:
                    lines += 1
                    self.assertEqual(row, TEST_DATA[i - 1])
        self.assertEqual(lines, len(TEST_DATA))



if __name__ == '__main__':
    main()
