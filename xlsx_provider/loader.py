#!/usr/bin/env python

import os
import os.path
import csv
import json
import datetime
from openpyxl import load_workbook, Workbook
from xlsx_provider.commons import DEFAULT_CSV_DELIMITER, XLS_EPOC

__all__ = ['load_worksheet']

loaders = {}


def extension(*exts):
    "File extensions decorator"

    def wrap(f):
        for ext in exts:
            loaders[ext] = f
        return f

    return wrap


@extension('csv')
def load_worksheet_csv(filename, skip_rows=0, csv_delimiter=None, **kwargs):
    "Load a worksheet from a CSV file"
    wb = Workbook()
    with open(filename, 'r', encoding='utf8') as f:
        reader = csv.reader(f, delimiter=csv_delimiter)
        for i, row in enumerate(reader):
            if i >= skip_rows:
                wb.active.append(row)
    return wb.active


@extension('xls', 'xlt')
def load_worksheet_xls(filename, skip_rows=0, worksheet=0, **kwargs):
    "Load a worksheet from an XLS file"
    import xlrd

    wb = xlrd.open_workbook(filename)
    if isinstance(worksheet, int):
        sheet = wb.sheets()[worksheet]
    else:  #  get by name
        t = [x for x in wb.sheet_names() if x.lower() == worksheet.lower()]
        if not t:
            raise KeyError('Worksheet {0} not found'.format(worksheet))
        sheet = t[0]
    # Prepare an XLSX sheet
    xsheet = Workbook().worksheets[0]
    for row in range(skip_rows, sheet.nrows):
        for col in range(0, sheet.ncols):
            value = sheet.cell_value(row, col)
            cell_type = sheet.cell_type(row, col)
            if cell_type == xlrd.XL_CELL_DATE:
                value = XLS_EPOC + datetime.timedelta(days=value)
            elif cell_type == xlrd.XL_CELL_NUMBER:
                if isinstance(value, float) and value.is_integer():
                    value = int(value)
                # print(value, type(value), sheet.cell_type(row, col))
            xsheet.cell(row=row + 1, column=col + 1).value = value
    assert xsheet.max_row == sheet.nrows
    return xsheet


@extension('xlsx', 'xlsm', 'xlsb')
def load_worksheet_xlsx(filename, skip_rows=0, worksheet=0, **kwargs):
    "Load a worksheet from an XLSX file"
    wb = load_workbook(filename=filename, data_only=True)
    if isinstance(worksheet, int):
        sheet = wb.worksheets[worksheet]
    else:  #  get by name
        t = [x for x in wb.worksheets if x.title.lower() == worksheet.lower()]
        if not t:
            raise KeyError('Worksheet {0} not found'.format(worksheet))
        sheet = t[0]
    if skip_rows:
        wb.delete_rows(idx=1, amount=skip_rows)
    return sheet


@extension('parquet')
def read_parquet(filename, skip_rows=0, **kwargs):
    "Load a worksheet from a Parquet file"
    import pandas as pd

    wb = Workbook()
    f = pd.read_parquet(filename)
    # header
    wb.active.append(list(f.columns))
    # rows
    for i, row in enumerate(f.values):
        if i >= skip_rows:
            wb.active.append(list(row))
    return wb.active


@extension('json')
def read_json(filename, skip_rows=0, **kwargs):
    "Load a worksheet from a JSON file"
    wb = Workbook()
    with open(filename, 'r', encoding='utf8') as f:
        try:
            data = json.load(f)
        except json.decoder.JSONDecodeError as ex:
            # Try load as JSON Lines
            try:
                f.seek(0)  # rewind
                data = json.loads('[' + f.read().replace('\n', ',') + ']')
            except json.decoder.JSONDecodeError:
                raise ex
        keys = list(data[0].keys())
        # header
        wb.active.append(keys)
        # rows
        for i, row in enumerate(data):
            if i >= skip_rows:
                wb.active.append([row.get(key) for key in keys])
    return wb.active


@extension('jsonl')
def read_jsonl(filename, skip_rows=0, **kwargs):
    "Load a worksheet from a JSON Lines file"
    wb = Workbook()
    with open(filename, 'r', encoding='utf8') as f:
        data = json.loads('[' + f.read().replace('\n', ',') + ']')
        keys = list(data[0].keys())
        # header
        wb.active.append(keys)
        # rows
        for i, row in enumerate(data):
            if i >= skip_rows:
                wb.active.append([row.get(key) for key in keys])
    return wb.active


def load_worksheet(
    filename, worksheet=0, skip_rows=0, csv_delimiter=DEFAULT_CSV_DELIMITER, ext=None
):
    """
    Load a worksheet from a supported file format

    :param worksheet: Worksheet title or number (zero-based)
    :type worksheet: str or int
    :param skip_rows: Number of input lines to skip
    :type skip_rows: int
    :param csv_delimiter: CSV delimiter
    :type csv_delimiter: str
    :param ext: Force file format (autodetect by default)
    :type ext: str
    """
    if ext is None:
        ext = os.path.splitext(filename)[1]
    ext = ext.lower().lstrip('.')
    loader = loaders.get(ext)
    if loader is None:
        raise KeyError('unsupported file format {}'.format(ext))
    return loader(
        filename=filename,
        skip_rows=skip_rows,
        worksheet=worksheet,
        csv_delimiter=csv_delimiter,
    )
