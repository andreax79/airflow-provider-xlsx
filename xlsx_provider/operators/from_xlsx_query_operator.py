#!/usr/bin/env python

import csv
import json
import sqlite3
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from xlsx_provider.commons import (
    get_type,
    quoted,
    col_number_to_name,
    FileFormat,
    DEFAULT_CSV_DELIMITER,
    DEFAULT_CSV_HEADER,
    DEFAULT_FORMAT,
    DEFAULT_TABLE_NAME,
    HEADER_UPPER,
    HEADER_LOWER,
)
from xlsx_provider.operators.from_xlsx_operator import FromXLSXOperator

__all__ = ['FromXLSXQueryOperator']


class FromXLSXQueryOperator(FromXLSXOperator):
    """
    Execute an SQL query an XLSX/XLS file and export the result into a Parquet or CSV file

    This operators loads an XLSX or XLS file into an in-memory SQLite database,
    executes a query on the db and stores the result into a Parquet, CSV, JSON, JSON Lines(one line per record) file.
    The output columns names and types are determinated by the SQL query output.

    :param source: source filename (XLSX or XLS, templated)
    :type source: str
    :param target: target filename (templated)
    :type target: str
    :param worksheet: worksheet title or number (zero-based, templated)
    :type worksheet: str or int
    :param types: force column type (dict or list column='str', 'd', 'datetime64[ns]')
    :type types: str or dictionary of string key/value pair
    :param file_format: output file format (parquet, csv, json, jsonl)
    :type file_format: str
    :param csv_delimiter: CSV delimiter (default: ',')
    :type csv_delimiter: str
    :param csv_header: convert CSV header case ('lower', 'upper', 'skip')
    :type csv_header: str
    :param query: SQL query (templated)
    :type query: str
    :param table_name: Table name (default: 'xls', templated)
    """

    FileFormat = FileFormat
    template_fields = ('source', 'target', 'worksheet', 'query', 'table_name')
    ui_color = '#a934bd'

    @apply_defaults
    def __init__(
        self,
        source,
        target,
        worksheet=0,
        types=None,
        file_format=DEFAULT_FORMAT,
        csv_delimiter=DEFAULT_CSV_DELIMITER,
        csv_header=DEFAULT_CSV_HEADER,
        query=None,
        table_name=DEFAULT_TABLE_NAME,
        *args,
        **kwargs
    ):
        super(FromXLSXQueryOperator, self).__init__(
            *args,
            **kwargs,
            source=source,
            target=target,
            worksheet=worksheet,
            types=types,
            file_format=file_format,
            csv_delimiter=csv_delimiter,
            csv_header=csv_header
        )
        self.query = query
        self.table_name = table_name

    def write_parquet(self, result):
        "Write the results in parquet format"
        import pandas as pd
        import pyarrow.parquet

        pd_data = {}
        for name in result.columns.keys():
            pd_data[name] = pd.Series(
                result.columns[name], dtype=result.datatypes[name]
            )
        df = pd.DataFrame(pd_data)
        pyarrow.parquet.write_table(
            table=pyarrow.Table.from_pandas(df),
            where=self.target,
            compression='SNAPPY',
            flavor='spark',
        )

    def write_csv(self, result):
        "Write data to CSV file"
        data = list(zip(*[result.columns[x] for x in result.columns.keys()]))
        with open(self.target, 'w') as f:
            csw_writer = csv.writer(
                f, quoting=csv.QUOTE_MINIMAL, delimiter=self.csv_delimiter
            )
            if self.csv_header == HEADER_UPPER:
                csw_writer.writerow([x.upper() for x in result.columns.keys()])
            elif self.csv_header == HEADER_LOWER:
                csw_writer.writerow([x.lower() for x in result.columns.keys()])
            csw_writer.writerows(data)

    def write_json(self, result):
        "Write data to JSON file"
        data = list(
            dict(q)
            for q in zip(*(list((k, x) for x in v) for k, v in result.columns.items()))
        )
        with open(self.target, 'w') as f:
            f.write(json.dumps(data, indent=2))

    def write_jsonl(self, result):
        "Write data to JSON Lines file"
        data = list(
            dict(q)
            for q in zip(*(list((k, x) for x in v) for k, v in result.columns.items()))
        )
        with open(self.target, 'w') as f:
            f.write('\n'.join(json.dumps(x) for x in data))

    def write(self, result):
        "Write data to file"
        if self.file_format == FileFormat.csv:
            self.write_csv(result)
        elif self.file_format == FileFormat.json:
            self.write_json(result)
        elif self.file_format == FileFormat.jsonl:
            self.write_jsonl(result)
        else:
            self.write_parquet(result)

    def execute(self, context):
        try:
            sheet = self.load_worksheet(filename=self.source, worksheet=self.worksheet)
            result = Result(self.table_name, sheet, self.types)
            result.process(self.query)
            self.write(result)
        except Exception as e:
            raise AirflowException("FromXLSXQueryOperator error: {0}".format(str(e)))
        return True


class Result(object):
    def __init__(self, table_name, sheet, types):
        self.table_name = table_name
        self.sheet = sheet
        self.types = types

    def process_row(self, row):
        for i, name in enumerate(self.columns_names):
            value = row[i]
            if isinstance(value, str):
                value = value.strip()
            self.columns[name].append(value)
            if self.datatypes[name] is None and value is not None:
                self.datatypes[name] = get_type(name, value)

    def process(self, query=None):
        detect_types = sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        with sqlite3.connect(':memory:', detect_types=detect_types) as conn:
            sql_columns = ','.join(
                (
                    quoted(col_number_to_name(x))
                    for x in range(0, self.sheet.max_column + 1)
                )
            )
            # Create table
            create_table_sql = 'create table {table}({columns})'.format(
                table=self.table_name, columns=sql_columns
            )
            conn.execute(create_table_sql)
            # Insert data
            insert_sql = 'insert into {table}({columns}) values({values})'.format(
                table=self.table_name,
                columns=sql_columns,
                values=','.join(['?' for x in range(0, self.sheet.max_column + 1)]),
            )
            conn.executemany(
                insert_sql,
                ((x[0],) + x[1] for x in enumerate(self.sheet.values, start=1)),
            )
            # Query
            result = conn.execute(query)
            # Process result
            self.columns_names = list(x[0].lower() for x in result.description)
            self.datatypes = dict(
                [(name, self.types.get(name)) for name in self.columns_names]
            )
            self.columns = dict([(name, []) for name in self.columns_names])
            for row in result:
                self.process_row(row)
            result.close()
