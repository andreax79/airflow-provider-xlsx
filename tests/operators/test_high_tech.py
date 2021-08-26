#!/usr/bin/env python

import os
import json
import os.path
import shutil
import tempfile
from unittest import TestCase, main
from xlsx_provider.operators.from_xlsx_query_operator import FromXLSXQueryOperator

TEST_DATA_JSON = [
    {"high_tech_sector": "Pharmacy", "value": 78280, "share": 0.231952169555313},
    {
        "high_tech_sector": "Electronics-telecommunications",
        "value": 75243,
        "share": 0.222954583130376,
    },
    {
        "high_tech_sector": "Scientific instruments",
        "value": 64010,
        "share": 0.189670433253542,
    },
    {"high_tech_sector": "Aerospace", "value": 44472, "share": 0.131776952366115},
    {
        "high_tech_sector": "Computers office machines",
        "value": 21772,
        "share": 0.0645136852766778,
    },
    {
        "high_tech_sector": "Non-electrical machinery",
        "value": 20813,
        "share": 0.0616714981835167,
    },
    {"high_tech_sector": "Chemistry", "value": 19776, "share": 0.058598734453222},
    {
        "high_tech_sector": "Electrical machinery",
        "value": 9730,
        "share": 0.028831912195612,
    },
    {"high_tech_sector": "Armament", "value": 3384, "share": 0.0100300315856265},
]


class TestFromQuery(TestCase):
    def setUp(self):
        self.root_dir = os.path.dirname(os.path.realpath(__file__))
        self.target_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.target_dir)

    def test_read_xlsx(self):
        source = os.path.join(self.root_dir, 'high_tech.xlsx')
        target = os.path.join(self.target_dir, 'high_tech.xlsx.json')
        so = FromXLSXQueryOperator(
            task_id='test',
            source=source,
            target=target,
            file_format='json',
            table_name='test',
            worksheet='Figure 3',
            query='''
                select
                    g as high_tech_sector,
                    cast(h * 1000 as int) as value,
                    i as share
                from
                    test
                where
                    _index > 1
                    and high_tech_sector <> ''
                    and lower(high_tech_sector) <> 'total'
            ''',
        )
        so.execute({})
        with open(target, 'r') as f:
            data = json.load(f)
        self.assertEqual(data, TEST_DATA_JSON)


if __name__ == '__main__':
    main()
