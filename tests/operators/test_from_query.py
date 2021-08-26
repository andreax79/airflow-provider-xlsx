#!/usr/bin/env python

import os
import csv
import os.path
import shutil
import tempfile
import json
from unittest import TestCase, main
from airflow.exceptions import AirflowException
from xlsx_provider.operators.from_xlsx_query_operator import FromXLSXQueryOperator

TEST_DATA = [
    ['One', '2021-09-07 00:00:00', '0.1', '1', '10', '0.1'],
    ['Two', '2011-09-08 00:00:00', '0.2', '0', '30', '0.2'],
    ['à§æ', '2011-09-09 00:00:00', '0.3', '1', '60', '0.4'],
]

TEST_DATA_JSON = [
    {
        'col0': 'One',
        'col1': '2021-09-07 00:00:00',
        'col2': 0.1,
        'col3': 1,
        'col4': 10,
        'col5': 0.1,
    },
    {
        'col0': 'Two',
        'col1': '2011-09-08 00:00:00',
        'col2': 0.2,
        'col3': 0,
        'col4': 30,
        'col5': 0.2,
    },
    {
        'col0': 'à§æ',
        'col1': '2011-09-09 00:00:00',
        'col2': 0.3,
        'col3': 1,
        'col4': 60,
        'col5': 0.4,
    },
]

QUERY = """
    select
        a as col0,
        b as col1,
        c/100.0 as col2,
        d as col3,
        e as col4,
        f*1000 as col5
    from
        test
    where
        _index > 1
"""


def expect_exception(exception):
    def test_decorator(fn):
        def test_decorated(self, *args, **kwargs):
            self.assertRaises(exception, fn, self, *args, **kwargs)

        return test_decorated

    return test_decorator


class TestFromQuery(TestCase):
    def setUp(self):
        self.root_dir = os.path.dirname(os.path.realpath(__file__))
        self.target_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.target_dir)

    def test_to_csv(self):
        source = os.path.join(self.root_dir, 'test.xlsx')
        target = os.path.join(self.target_dir, 'test.xlsx.csv')
        so = FromXLSXQueryOperator(
            task_id='test',
            source=source,
            target=target,
            csv_delimiter='|',
            file_format='csv',
            table_name='test',
            query=QUERY,
        )
        so.execute({})
        with open(target, 'r') as f:
            reader = csv.reader(f, delimiter='|')
            for i, row in enumerate(reader):
                if i >= 1:
                    self.assertEqual(row, TEST_DATA[i - 1])

    def test_to_json(self):
        source = os.path.join(self.root_dir, 'test.xlsx')
        target = os.path.join(self.target_dir, 'test.xlsx.json')
        so = FromXLSXQueryOperator(
            task_id='test',
            source=source,
            target=target,
            csv_delimiter='|',
            file_format='json',
            table_name='test',
            query=QUERY,
        )
        so.execute({})
        with open(target, 'r') as f:
            data = json.load(f)
            self.assertEqual(data, TEST_DATA_JSON)

    def test_to_jsonl(self):
        source = os.path.join(self.root_dir, 'test.xlsx')
        target = os.path.join(self.target_dir, 'test.xlsx.jsonl')
        so = FromXLSXQueryOperator(
            task_id='test',
            source=source,
            target=target,
            csv_delimiter='|',
            file_format='jsonl',
            table_name='test',
            query=QUERY,
        )
        so.execute({})
        with open(target, 'r') as f:
            data = json.loads('[' + f.read().replace('\n', ',') + ']')
            self.assertEqual(data, TEST_DATA_JSON)

    @expect_exception(AirflowException)
    def test_invalid_worksheet(self):
        source = os.path.join(self.root_dir, 'test.xlsx')
        target = os.path.join(self.target_dir, 'test.xlsx.csv')
        so = FromXLSXQueryOperator(
            task_id='test',
            source=source,
            target=target,
            csv_delimiter='|',
            file_format='csv',
            table_name='test',
            worksheet='bla',
            query=QUERY,
        )
        so.execute({})


if __name__ == '__main__':
    main()
