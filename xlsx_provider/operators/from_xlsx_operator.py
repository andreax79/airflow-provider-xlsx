#!/usr/bin/env python

import json
import datetime
import dateutil.parser
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from xlsx_provider.loader import load_worksheet
from xlsx_provider.commons import (
    check_column_names,
    get_type,
    get_column_names,
    FileFormat,
    DEFAULT_CSV_DELIMITER,
    DEFAULT_CSV_HEADER,
    DEFAULT_FORMAT,
    INDEX_COLUMN_NAME,
    HEADER_UPPER,
    HEADER_LOWER,
    XLSX_EPOC,
)

__all__ = ['FromXLSXOperator']


class FromXLSXOperator(BaseOperator):
    """
    Convert an XLSX/XLS file into Parquet or CSV file

    Read an XLSX or XLS file and convert it into Parquet, CSV, JSON, JSON Lines(one line per record) file.

    :param source: Source filename (XLSX or XLS, templated)
    :type source: str
    :param target: Target filename (templated)
    :type target: str
    :param worksheet: Worksheet title or number (zero-based, templated)
    :type worksheet: str or int
    :param skip_rows: Number of input lines to skip (default: 0, templated)
    :type skip_rows: int
    :param limit: Row limit (default: None, templated)
    :type limit: int
    :param drop_columns: List of columns to be dropped
    :type drop_columns: list of str
    :param add_columns: Columns to be added (dict or list column=value)
    :type add_columns: list of str or dictionary of string key/value pair
    :param types: force Parquet column types (dict or list column='str', 'd', 'datetime64[ns]')
    :type types: str or dictionary of string key/value pair
    :param column_names: force columns names (list)
    :type column_names: list of str
    :param file_format: Output file format (parquet, csv, json, jsonl)
    :type file_format: str
    :param csv_delimiter: CSV delimiter (default: ',')
    :type csv_delimiter: str
    :param csv_header: Convert CSV output header case ('lower', 'upper', 'skip')
    :type csv_header: str
    """

    FileFormat = FileFormat
    template_fields = ('source', 'target', 'worksheet', 'limit', 'skip_rows')
    ui_color = '#a934bd'

    @apply_defaults
    def __init__(
        self,
        source,
        target,
        worksheet=0,
        skip_rows=0,
        limit=None,
        drop_columns=None,
        add_columns=None,
        types=None,
        column_names=None,
        file_format=DEFAULT_FORMAT,
        csv_delimiter=DEFAULT_CSV_DELIMITER,
        csv_header=DEFAULT_CSV_HEADER,
        *args,
        **kwargs
    ):
        super(FromXLSXOperator, self).__init__(*args, **kwargs)
        self.source = source
        self.target = target
        try:
            self.worksheet = int(worksheet)
        except:
            self.worksheet = worksheet
        self.skip_rows = skip_rows
        self.limit = limit
        self.drop_columns = drop_columns or []
        if isinstance(add_columns, list):
            self.add_columns = dict(x.split('=') for x in add_columns)
        else:
            self.add_columns = add_columns or {}
        if isinstance(types, list):
            self.types = dict(x.split('=') for x in types)
        else:
            self.types = types or {}
        self.names = column_names
        self.file_format = FileFormat.lookup(file_format)
        self.csv_delimiter = csv_delimiter
        self.csv_header = csv_header

    def load_worksheet(self):
        # Load a worksheet
        return load_worksheet(
            filename=self.source,
            worksheet=self.worksheet,
            skip_rows=self.skip_rows,
            csv_delimiter=self.csv_delimiter,
        )

    def execute(self, context):
        try:
            sheet = self.load_worksheet()
            rows = list(sheet)
            if self.names is not None:
                names = self.names
            else:
                names = get_column_names(sheet, skip_rows=self.skip_rows)
            # Check unique columns
            check_column_names(names)
            datatypes = dict([(name, self.types.get(name)) for name in names])
            if INDEX_COLUMN_NAME in datatypes:
                datatypes[INDEX_COLUMN_NAME] = 'd'
            columns = dict([(name, []) for name in names])
            for name, value in self.add_columns.items():
                datatypes[name] = get_type(name, value)
                columns[name] = []
            for _index, row in enumerate(rows[1:]):
                if self.limit is not None and _index >= self.limit:
                    break
                for i, name in enumerate(names):
                    if name == INDEX_COLUMN_NAME:
                        columns[INDEX_COLUMN_NAME].append(_index)
                        continue
                    cel = row[i]
                    value = cel.value
                    if isinstance(value, str):
                        value = value.strip()
                    if datatypes[name] is None and value is not None:
                        datatypes[name] = get_type(name, value)
                    if datatypes[name] == 'datetime64[ns]':
                        if not value:
                            value = None
                        elif isinstance(value, int) or isinstance(value, float):
                            value = XLSX_EPOC + datetime.timedelta(days=value)
                        elif not isinstance(
                            value, datetime.datetime
                        ) and not isinstance(value, datetime.date):
                            value = dateutil.parser.parse(value)
                    columns[name].append(value)
                for name, value in self.add_columns.items():
                    if name not in names:
                        columns[name].append(value)
            row_num = sheet.max_row - 1
            if self.limit is not None:
                row_num = min(row_num, self.limit)
            for i, name in enumerate(names):
                assert len(columns[name]) == row_num  # check rows number (skip header)
            self.write(names, columns, datatypes)
        except Exception as e:
            raise AirflowException("XLSXToParquet operator error: {0}".format(str(e)))
        return True

    def to_dataframe(self, names, columns, datatypes):
        import pandas as pd

        all_names = names + [x for x in self.add_columns.keys() if x not in names]
        pd_data = {}
        for name in all_names:
            if name not in self.drop_columns:
                pd_data[name] = pd.Series(columns[name], dtype=datatypes[name])
        return pd.DataFrame(pd_data)

    def write_parquet(self, names, columns, datatypes):
        "Write the results in parquet format"
        import pyarrow.parquet

        df = self.to_dataframe(names, columns, datatypes)
        pyarrow.parquet.write_table(
            table=pyarrow.Table.from_pandas(df),
            where=self.target,
            compression='SNAPPY',
            flavor='spark',
        )

    def write_csv(self, names, columns, datatypes):
        "Write data to CSV file"
        with open(self.target, 'w') as f:
            # header
            if self.csv_header == HEADER_UPPER:
                f.write(self.csv_delimiter.join([x.upper() for x in datatypes.keys()]))
                f.write('\n')
            elif self.csv_header == HEADER_LOWER:
                f.write(self.csv_delimiter.join(datatypes.keys()))
                f.write('\n')
            # data
            df = self.to_dataframe(names, columns, datatypes)
            df.to_csv(
                path_or_buf=f,
                sep=self.csv_delimiter,
                header=False,
                index=False,
                date_format='%Y-%m-%d %M:%M:%S',
                float_format='%g'
            )

    def write_json(self, names, columns, datatypes):
        "Write data to JSON file"
        data = list(
            dict(q) for q in zip(*(list((k, x) for x in v) for k, v in columns.items()))
        )
        with open(self.target, 'w') as f:
            f.write(json.dumps(data, indent=2, default=str))

    def write_jsonl(self, names, columns, datatypes):
        "Write data to JSON Lines file"
        data = list(
            dict(q) for q in zip(*(list((k, x) for x in v) for k, v in columns.items()))
        )
        with open(self.target, 'w') as f:
            f.write('\n'.join(json.dumps(x, default=str) for x in data))

    def write(self, names, columns, datatypes):
        "Write data to file"
        if self.file_format == FileFormat.csv:
            self.write_csv(names, columns, datatypes)
        elif self.file_format == FileFormat.json:
            self.write_json(names, columns, datatypes)
        elif self.file_format == FileFormat.jsonl:
            self.write_jsonl(names, columns, datatypes)
        else:
            self.write_parquet(names, columns, datatypes)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('filename')
    parser.add_argument('-w', '--worksheet', dest='worksheet', default=0)
    parser.add_argument('-a', '--add_col', dest='add_columns', action='append')
    parser.add_argument('-d', '--drop_col', dest='drop_columns', action='append')
    parser.add_argument('-t', '--type', dest='types', action='append')
    parser.add_argument(
        '--delimiter', dest='csv_delimiter', default=DEFAULT_CSV_DELIMITER
    )
    parser.add_argument('-o', '--output', dest='output')
    parser.add_argument(
        '--header',
        dest='csv_header',
        choices=['lower', 'upper', 'skip'],
        default=DEFAULT_CSV_HEADER,
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--csv', dest='file_format_csv', action='store_true')
    group.add_argument(
        '--parquet', dest='file_format_csv', action='store_false', default=False
    )
    args = parser.parse_args()
    file_format = 'csv' if args.file_format_csv else 'parquet'
    so = FromXLSXOperator(
        task_id='test',
        source=args.filename,
        target=args.output or (args.filename + '.' + file_format),
        drop_columns=args.drop_columns,
        add_columns=args.add_columns,
        types=args.types,
        file_format=file_format,
        worksheet=args.worksheet,
        csv_delimiter=args.csv_delimiter,
        csv_header=args.csv_header,
    )
    so.execute({})
