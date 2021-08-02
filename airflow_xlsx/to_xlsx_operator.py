#!/usr/bin/env python

import csv
from openpyxl import Workbook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow_xlsx.commons import FileFormat, DEFAULT_CSV_DELIMITER

__all__ = ['ToXLSXOperator']


class ToXLSXOperator(BaseOperator):
    FileFormat = FileFormat

    template_fields = ('source', 'target')

    @apply_defaults
    def __init__(
        self, source, target, csv_delimiter=DEFAULT_CSV_DELIMITER, *args, **kwargs
    ):
        """
        :param source: source filename (csv or parquest, detected by the extension)
        :param target: target filename (xlsx)
        :param csv_delimiter: CSV delimiter (default: ',')
        """
        super(ToXLSXOperator, self).__init__(*args, **kwargs)
        self.source = source
        self.target = target
        self.csv_delimiter = csv_delimiter

    def execute(self, context):
        wb = Workbook()
        ws1 = wb.active
        if self.source.endswith('.parquet'):
            import pandas as pd

            f = pd.read_parquet(self.source)
            for row in f.values:
                ws1.append(list(row))
        else:
            with open(self.source, 'r', encoding='utf8') as f:
                reader = csv.reader(f, delimiter=self.csv_delimiter)
                for row in reader:
                    ws1.append(row)
        wb.save(self.target)
        return True
