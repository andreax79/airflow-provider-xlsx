#!/usr/bin/env python

import os
import os.path
import re
import datetime
import unicodedata
from enum import Enum

__all__ = [
    'check_column_names',
    'get_column_names',
    'get_type',
    'clean_key',
    'col_number_to_name',
    'quoted',
    'rmdiacritics',
    'FileFormat',
    'HEADER_LOWER',
    'HEADER_UPPER',
    'HEADER_SKIP',
    'DEFAULT_FORMAT',
    'DEFAULT_CSV_DELIMITER',
    'DEFAULT_CSV_HEADER',
    'DEFAULT_TABLE_NAME',
    'INDEX_COLUMN_NAME',
    'XLS_EPOC',
    'XLSX_EPOC',
    'VERSION',
]

HEADER_LOWER = 'lower'
HEADER_UPPER = 'upper'
HEADER_SKIP = 'skip'
#: Default output format
DEFAULT_FORMAT = 'parquet'
#: Default CSV delimiter
DEFAULT_CSV_DELIMITER = ','
#: Default CSV header case
DEFAULT_CSV_HEADER = HEADER_LOWER
#: XLS Epoc - see https://support.microsoft.com/en-us/help/214326/excel-incorrectly-assumes-that-the-year-1900-is-a-leap-year
XLS_EPOC = datetime.datetime(1899, 12, 30)
#: XLSX Epoc
XLSX_EPOC = datetime.datetime(1900, 1, 1)
#: Default Query Operator table name
DEFAULT_TABLE_NAME = 'xls'
#: Index colummn name
INDEX_COLUMN_NAME = '_index'


VERSION_FILE = os.path.join(os.path.dirname(__file__), "VERSION")
with open(VERSION_FILE) as f:
    #: Plugin Version
    VERSION = f.read().strip()


def rmdiacritics(char):
    "Return the base character without diacritics (eg. accents)"
    desc = unicodedata.name(char)
    cutoff = desc.find(' WITH ')
    if cutoff != -1:
        desc = desc[:cutoff]
        try:
            char = unicodedata.lookup(desc)
        except KeyError:
            pass
    return char


def quoted(string):
    return "'" + string + "'"


def clean_key(k):
    k = k.strip().lower().replace('-', '').replace('€', '')
    k = re.sub('[\ \'\<\>\(\)\.\,\/\_]+', '_', k)
    k = ''.join([rmdiacritics(x) for x in k])
    k = k.strip('_')
    return k


def col_number_to_name(col_number):
    """
    Convert a column number to name (e.g. 0 -> '_index', 0 -> A, 1 -> B)

    :param col_number: column number
    :type col_number: int
    """

    def _col_number_to_name(x):
        return (_col_number_to_name((x // 26) - 1) if x >= 26 else '') + chr(
            65 + (x % 26)
        )

    if col_number == 0:
        return INDEX_COLUMN_NAME
    else:
        return _col_number_to_name(col_number - 1)


def get_type(name, value):
    if isinstance(value, float) or isinstance(value, int):
        return 'd'  # double
    elif isinstance(value, datetime.datetime):
        return 'datetime64[ns]'  # datetime
    elif isinstance(value, str):
        return 'str'
    else:
        raise Exception('unsupported data type {} {}'.format(name, type(value)))


def get_column_names(sheet, skip_rows=0):
    """
    Extract the column names from the first row of the worksheet

    :param sheet: worksheet
    :type sheet: Worksheet
    :param skip_rows: Number of input lines to skip
    :type skip_rows: int
    """
    header = sheet[1 + skip_rows]
    names = [clean_key(x.value) for x in header if x.value is not None]
    # Append the column to the name if the name is not unique
    return [
        x
        if (i == 0 or x not in names[:i])
        else '{}_{}'.format(x, col_number_to_name(i + 1).lower())
        for i, x in enumerate(names)
    ]


def check_column_names(column_names):
    # Check unique columns
    if len(set(column_names)) != len(column_names):
        duplicates = list(set([x for x in column_names if column_names.count(x) > 1]))
        raise Exception('Columns names are not unique: {0}'.format(duplicates))


class FileFormat(Enum):
    "File format enumerator (parquet/csv/json/jsonl)"
    parquet = 'parquet'
    csv = 'csv'
    json = 'json'
    jsonl = 'jsonl'  # JSON lines (newline-delimited JSON)

    @classmethod
    def lookup(cls, file_format):
        if not file_format:
            return DEFAULT_FORMAT
        elif isinstance(file_format, FileFormat):
            return file_format
        else:
            return cls[file_format.lower()]
