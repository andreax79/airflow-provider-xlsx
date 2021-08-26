# Airflow Provider XLSX

[Apache Airflow](https://github.com/apache/airflow) operators for converting XLSX files from/to Parquet, CSV and JSON.

[![Build Status](https://github.com/andreax79/airflow-provider-xlsx/workflows/Tests/badge.svg)](https://github.com/andreax79/airflow-provider-xlsx/actions)
[![PyPI version](https://badge.fury.io/py/airflow-provider-xlsx.svg)](https://badge.fury.io/py/airflow-provider-xlsx)
[![PyPI](https://img.shields.io/pypi/pyversions/airflow-provider-xlsx.svg)](https://pypi.org/project/airflow-provider-xlsx)
[![Downloads](https://pepy.tech/badge/airflow-provider-xlsx/month)](https://pepy.tech/project/airflow-provider-xlsx)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

### System Requirements

* Airflow Versions
    * 2.0 or newer

### Installation

```console
$ pip install airflow-provider-xlsx
```

### Operators

#### FromXLSXOperator

Read an XLSX or XLS file and convert it into Parquet, CSV, JSON, JSON Lines(one line per record) file.

[API Documentation](https://airflow-provider-xlsx.readthedocs.io/en/latest/#module-xlsx_provider.operators.from_xlsx_operator)

##### Example

XLSX Source

![image](https://user-images.githubusercontent.com/1288154/130972144-e33f01af-2f9a-4e34-803a-907324a7adbf.png)

Airflow Task

```python
from xlsx_provider.operators.from_xlsx_operator import FromXLSXOperator

xlsx_to_jsonl = FromXLSXOperator(
   task_id='xlsx_to_jsonl',
   source='{{ var.value.tmp_path }}/test.xlsx',
   target='{{ var.value.tmp_path }}/test.jsonl',
   file_format='jsonl',
   dag=dag
)
```

JSON Lines Output

```json
{"month": "Jan", "high": -12.2, "mean": -16.2, "low": -20.1, "precipitation": 19}
{"month": "Feb", "high": -10.3, "mean": -14.7, "low": -19.1, "precipitation": 14}
{"month": "Mar", "high": -2.6, "mean": -7.2, "low": -11.8, "precipitation": 15}
{"month": "Apr", "high": 8.1, "mean": 3.2, "low": -1.7, "precipitation": 24}
{"month": "May", "high": 17.5, "mean": 11.6, "low": 5.6, "precipitation": 36}
{"month": "Jun", "high": 24, "mean": 18.2, "low": 12.3, "precipitation": 58}
{"month": "Jul", "high": 25.7, "mean": 20.2, "low": 14.7, "precipitation": 72}
{"month": "Aug", "high": 22.2, "mean": 17, "low": 11.7, "precipitation": 66}
{"month": "Sep", "high": 16.6, "mean": 11.5, "low": 6.4, "precipitation": 44}
{"month": "Oct", "high": 6.8, "mean": 3.4, "low": 0, "precipitation": 38}
```

#### FromXLSXQueryOperator

Execute an SQL query an XLSX/XLS file and export the result into a Parquet or CSV file

This operators loads an XLSX or XLS file into an in-memory SQLite database, executes a query on the db and stores the result into a Parquet, CSV, JSON, JSON Lines(one line per record) file. The output columns names and types are determinated by the SQL query output.

[API Documentation](https://airflow-provider-xlsx.readthedocs.io/en/latest/#xlsx-provider-operators-operators-from-xlsx-query-operator)

##### Example

XLSX Source

![image](https://user-images.githubusercontent.com/1288154/130963470-f7f05ca0-a952-47e1-86ec-c6cd322746f6.png)

SQL Query

```sql
 select
     g as high_tech_sector,
     h as eur_bilion,
     i as share
 from
     high_tech
 where
     _index > 1
     and high_tech_sector <> ''
     and lower(high_tech_sector) <> 'total'
```

Airflow Task

```python
from xlsx_provider.operators.from_xlsx_query_operator import FromXLSXQueryOperator

xlsx_to_csv = FromXLSXQueryOperator(
   task_id='xlsx_to_csv',
   source='{{ var.value.tmp_path }}/high_tech.xlsx',
   target='{{ var.value.tmp_path }}/high_tech.parquet',
   file_format='csv',
   csv_delimiter=',',
   table_name='high_tech',
   worksheet='Figure 3',
   query='''
       select
           g as high_tech_sector,
           h as eur_bilion,
           i as share
       from
           high_tech
       where
           _index > 1
           and high_tech_sector <> ''
           and lower(high_tech_sector) <> 'total'
   ''',
   dag = dag
)
```

Output

```
high_tech_sector,value,share
Pharmacy,78280,0.231952169555313
Electronics-telecommunications,75243,0.222954583130376
Scientific instruments,64010,0.189670433253542
Aerospace,44472,0.131776952366115
Computers office machines,21772,0.0645136852766778
Non-electrical machinery,20813,0.0616714981835167
Chemistry,19776,0.058598734453222
Electrical machinery,9730,0.028831912195612
Armament,3384,0.0100300315856265
```

#### ToXLSXOperator

Read a Parquest, CSV, JSON, JSON Lines(one line per record) file and convert it into XLSX.

[API Documentation](https://airflow-provider-xlsx.readthedocs.io/en/latest/#xlsx-provider-operators-operators-to-xlsx-operator)

##### Example

```python
from xlsx_provider.operators.to_xlsx_operator import ToXLSXOperator

parquet_to_xlsx = ToXLSXOperator(
   task_id='parquet_to_xlsx',
   source='{{ var.value.tmp_path }}/test.parquet',
   target='{{ var.value.tmp_path }}/test.xlsx',
   dag=dag
)

```

### Links

* Apache Airflow - https://github.com/apache/airflow
* Project home page (GitHub) - https://github.com/andreax79/airflow-provider-xlsx
* Documentation (Read the Docs) - https://airflow-provider-xlsx.readthedocs.io/en/latest
* openpyxl, library to read/write Excel 2010 xlsx/xlsm/xltx/xltm files - https://foss.heptapod.net/openpyxl/openpyxl
* lrd, library for reading data and formatting information from Excel files in the historical .xls format - https://github.com/python-excel/xlrd
* Python library for Apache Arrow - https://github.com/apache/arrow/tree/master/python
