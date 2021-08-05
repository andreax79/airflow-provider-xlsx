#!/usr/bin/env python

import os
import csv
import os.path
import shutil
import tempfile
from unittest import TestCase, main
from xlsx_provider.operators.from_xlsx_query_operator import FromXLSXQueryOperator

TEST_DATA = [
    ['One', '2021-09-07 00:00:00', '0.1', '1', '10', '0.1'],
    ['Two', '2011-09-08 00:00:00', '0.2', '0', '30', '0.2'],
    ['à§æ', '2011-09-09 00:00:00', '0.3', '1', '60', '0.4'],
]


class TestFromQuery(TestCase):
    def setUp(self):
        self.root_dir = os.path.dirname(os.path.realpath(__file__))
        self.target_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.target_dir)

    def test_read_xlsx(self):
        source = os.path.join(self.root_dir, 'test.xlsx')
        target = os.path.join(self.root_dir, 'test.xlsx.csv')
        so = FromXLSXQueryOperator(
            task_id='test',
            source=source,
            target=target,
            csv_delimiter='|',
            file_format='csv',
            table_name='test',
            query='''
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
            ''',
        )
        so.execute({})
        with open(target, 'r') as f:
            reader = csv.reader(f, delimiter='|')
            for i, row in enumerate(reader):
                if i >= 1:
                    self.assertEqual(row, TEST_DATA[i - 1])


if __name__ == '__main__':
    main()
