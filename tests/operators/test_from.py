#!/usr/bin/env python

import os
import csv
import json
import os.path
import shutil
import tempfile
import pyarrow.parquet
from unittest import TestCase, main
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


class TestFrom(TestCase):
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

        # target2 = os.path.join(self.root_dir, 'test1.xls.json')
        # so = FromXLSXOperator(
        #     task_id='test',
        #     source=target,
        #     target=target2,
        #     csv_delimiter='|',
        #     types={ 'col3number': 'd', 'col4boolean': 'd', 'col4numformula': 'd' },
        #     file_format='json',
        # )
        # so.execute({})
        # with open(target2, 'r') as f:
        #     data = json.load(f)
        #     self.assertEqual(data, TEST_DATA_JSON)

    def test_xls_to_parquet(self):
        source = os.path.join(self.root_dir, 'test.xls')
        target = os.path.join(self.target_dir, 'test.xls.parquet')
        so = FromXLSXOperator(
            task_id='test',
            source=source,
            target=target,
            file_format='parquet',
        )
        so.execute({})
        p = pyarrow.parquet.read_table(target).to_pandas()
        self.assertEqual(len(p), 3)
        self.assertTrue(all(p['col3number'] == [10, 20, 30]))
        self.assertTrue(all(p['col4numformula'] == [10, 30, 60]))

        target2 = os.path.join(self.target_dir, 'test.xls.json')
        so = FromXLSXOperator(
            task_id='test',
            source=target,
            target=target2,
            file_format='json',
        )
        so.execute({})
        with open(target2, 'r') as f:
            data = json.load(f)
            self.assertEqual(data, TEST_DATA_JSON)

    def test_xls_to_json(self):
        source = os.path.join(self.root_dir, 'test.xls')
        target = os.path.join(self.target_dir, 'test.xls.json')
        so = FromXLSXOperator(
            task_id='test',
            source=source,
            target=target,
            limit=10,
            file_format='json',
        )
        so.execute({})
        with open(target, 'r') as f:
            data = json.load(f)
            self.assertEqual(data, TEST_DATA_JSON)

    def test_xls_to_jsonl(self):
        source = os.path.join(self.root_dir, 'test.xls')
        target = os.path.join(self.target_dir, 'test.xls.jsonl')
        so = FromXLSXOperator(
            task_id='test',
            source=source,
            target=target,
            limit=10,
            file_format='jsonl',
        )
        so.execute({})
        with open(target, 'r') as f:
            data = json.loads('[' + f.read().replace('\n', ',') + ']')
            self.assertEqual(data, TEST_DATA_JSON)

        target2 = os.path.join(self.target_dir, 'test.xls.json')
        so = FromXLSXOperator(
            task_id='test',
            source=target,
            target=target2,
            file_format='json',
        )
        so.execute({})
        with open(target2, 'r') as f:
            data = json.load(f)
            self.assertEqual(data, TEST_DATA_JSON)


if __name__ == '__main__':
    main()
