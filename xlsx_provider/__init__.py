import os
import os.path

__all__ = ['get_provider_info']


def get_provider_info():
    version_file = os.path.join(os.path.dirname(__file__), "VERSION")
    with open(version_file) as f:
        version = f.read().strip()

    return {
        "package-name": "airflow-provider-xlsx",  # Required
        "name": "Airflow XLSX Provider",  # Required
        "description": "Airflow operators for converting XLSX files from/to Parquet/CSV",  # Required
        "hook-class-names": [],
        "extra-links": [],
        "versions": [version],  # Required
    }
