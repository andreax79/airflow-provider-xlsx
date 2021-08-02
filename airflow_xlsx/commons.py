#!/usr/bin/env python

import os
import os.path
import re
import datetime
import unicodedata
from enum import Enum

HEADER_LOWER = 'lower'
HEADER_UPPER = 'upper'
HEADER_SKIP = 'skip'
DEFAULT_FORMAT = 'parquet'
DEFAULT_CSV_DELIMITER = ','
DEFAULT_CSV_HEADER = HEADER_LOWER
# https://support.microsoft.com/en-us/help/214326/excel-incorrectly-assumes-that-the-year-1900-is-a-leap-year
XLS_EPOC = datetime.datetime(1899, 12, 30)
XLSX_EPOC = datetime.datetime(1900, 1, 1)

__all__ = [
    'rmdiacritics',
    'clean_key',
    'FileFormat',
    'HEADER_LOWER',
    'HEADER_UPPER',
    'HEADER_SKIP',
    'DEFAULT_FORMAT',
    'DEFAULT_CSV_DELIMITER',
    'DEFAULT_CSV_HEADER',
    'XLS_EPOC',
    'XLSX_EPOC',
    'VERSION',
]

VERSION_FILE = os.path.join(os.path.dirname(__file__), "VERSION")
with open(VERSION_FILE) as f:
    VERSION = f.read().strip()


def rmdiacritics(char):
    # Return the base character without diacritics (eg. accents)
    desc = unicodedata.name(char)
    cutoff = desc.find(' WITH ')
    if cutoff != -1:
        desc = desc[:cutoff]
        try:
            char = unicodedata.lookup(desc)
        except KeyError:
            pass
    return char


def clean_key(k):
    k = k.strip().lower().replace('-', '').replace('€', '')
    k = re.sub('[\ \'\<\>\(\)\.\,\/\_]+', '_', k)
    k = ''.join([rmdiacritics(x) for x in k])
    k = k.strip('_')
    return k


class FileFormat(Enum):
    parquet = 'parquet'
    csv = 'csv'

    @classmethod
    def lookup(cls, file_format):
        if not file_format:
            return DEFAULT_FORMAT
        elif isinstance(file_format, FileFormat):
            return file_format
        else:
            return cls[file_format.lower()]
