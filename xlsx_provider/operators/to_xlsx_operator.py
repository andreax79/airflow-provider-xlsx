#!/usr/bin/env python

import csv
from openpyxl import Workbook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from xlsx_provider.commons import FileFormat, DEFAULT_CSV_DELIMITER

__all__ = ['ToXLSXOperator']


class ToXLSXOperator(BaseOperator):
    """
    Convert a Parquet or CSV file into XLSX or XLS

    :param source: source filename (csv or parquet, detected by the extension, templated)
    :type source: str
    :param target: target filename (xlsx, templated)
    :type target: str
    :param csv_delimiter: CSV delimiter (default: ',')
    :type csv_delimiter: str
    """

    FileFormat = FileFormat
    template_fields = ('source', 'target')

    @apply_defaults
    def __init__(
        self, source, target, csv_delimiter=DEFAULT_CSV_DELIMITER, *args, **kwargs
    ):
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
