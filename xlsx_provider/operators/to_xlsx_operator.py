#!/usr/bin/env python

from openpyxl import Workbook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from xlsx_provider.loader import load_worksheet
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
    :param skip_rows: Number of input lines to skip (default: 0, templated)
    :type skip_rows: int
    """

    FileFormat = FileFormat
    template_fields = ('source', 'target', 'skip_rows')
    ui_color = '#a934bd'

    @apply_defaults
    def __init__(
        self,
        source,
        target,
        worksheet=0,
        skip_rows=0,
        csv_delimiter=DEFAULT_CSV_DELIMITER,
        *args,
        **kwargs
    ):
        super(ToXLSXOperator, self).__init__(*args, **kwargs)
        self.source = source
        self.target = target
        try:
            self.worksheet = int(worksheet)
        except:
            self.worksheet = worksheet
        self.skip_rows = skip_rows
        self.csv_delimiter = csv_delimiter

    def load_worksheet(self):
        # Load a worksheet
        return load_worksheet(
            filename=self.source,
            worksheet=self.worksheet,
            skip_rows=self.skip_rows,
            csv_delimiter=self.csv_delimiter,
        )

    def execute(self, context):
        sheet = self.load_worksheet()
        # Create a new workbook and append the loaded sheet
        wb = Workbook()
        wb.worksheets.clear()
        wb.worksheets.append(sheet)
        wb.save(self.target)
        return True
