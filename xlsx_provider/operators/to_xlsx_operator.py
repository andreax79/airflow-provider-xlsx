#!/usr/bin/env python

import csv
import json
from openpyxl import Workbook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from xlsx_provider.commons import FileFormat, DEFAULT_CSV_DELIMITER

__all__ = ['ToXLSXOperator']


class ToXLSXOperator(BaseOperator):
    """
    Convert Parquest, CSV, JSON, JSON Lines into XLSX

    Read a Parquest, CSV, JSON, JSON Lines(one line per record) file and convert it into XLSX

    :param source: source filename (type is detected by the extension, templated)
    :type source: str
    :param target: target filename (templated)
    :type target: str
    :param csv_delimiter: CSV delimiter (default: ',')
    :type csv_delimiter: str
    """

    FileFormat = FileFormat
    template_fields = ('source', 'target')
    ui_color = '#a934bd'

    @apply_defaults
    def __init__(
        self, source, target, csv_delimiter=DEFAULT_CSV_DELIMITER, *args, **kwargs
    ):
        super(ToXLSXOperator, self).__init__(*args, **kwargs)
        self.source = source
        self.target = target
        self.csv_delimiter = csv_delimiter

    def execute(self, context):
        if self.source.endswith('.parquet'):
            wb = self.read_parquet()
        elif self.source.endswith('.json'):
            wb = self.read_json()
        elif self.source.endswith('.jsonl'):
            wb = self.read_jsonl()
        else:
            wb = self.read_csv()
        wb.save(self.target)
        return True

    def read_parquet(self):
        import pandas as pd

        wb = Workbook()
        f = pd.read_parquet(self.source)
        # header
        wb.active.append(list(f.columns))
        # rows
        for row in f.values:
            wb.active.append(list(row))
        return wb

    def read_csv(self):
        wb = Workbook()
        with open(self.source, 'r', encoding='utf8') as f:
            reader = csv.reader(f, delimiter=self.csv_delimiter)
            for row in reader:
                wb.active.append(row)
        return wb

    def read_json(self):
        wb = Workbook()
        with open(self.source, 'r', encoding='utf8') as f:
            data = json.load(f)
            keys = list(data[0].keys())
            # header
            wb.active.append(keys)
            # rows
            for row in data:
                wb.active.append([row.get(key) for key in keys])
        return wb

    def read_jsonl(self):
        wb = Workbook()
        with open(self.source, 'r', encoding='utf8') as f:
            data = json.loads('[' + f.read().replace('\n', ',') + ']')
            keys = list(data[0].keys())
            # header
            wb.active.append(keys)
            # rows
            for row in data:
                wb.active.append([row.get(key) for key in keys])
        return wb
