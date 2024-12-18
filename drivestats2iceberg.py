import argparse
import os
import re

from datetime import date
from io import BytesIO
from time import sleep
from zipfile import ZipFile

import pyarrow
import pyarrow as pa
import pyarrow.csv as csv
import requests
from pyarrow.lib import ArrowInvalid

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog

from dotenv import load_dotenv
from pyiceberg.table import Table

# Destination bucket and prefix
DST_BUCKET = "drivestats-iceberg"
NAMESPACE = "drivestats"
TABLE_NAME = "observations"

# Number of times to retry the write_table operation
WRITE_TABLE_RETRIES = 5

# Match the CSV filenames within the zip files
# Various forms:
# 2014/2014-06-25.csv
# data_Q1_2016/2016-03-08.csv
# 2017-03-01.csv
FILENAME_PATTERN = re.compile(r"^(?:.*/)?(\d\d\d\d)-(\d\d)-(\d\d).csv$")

# First three years had annual data
ANNUAL_DATA = [2013, 2014, 2015]
# Quarterly data starts Q1 2016
FIRST_YEAR_OF_QUARTERLY_DATA = 2016
# Path to Drives Stats files
DRIVE_STATS_URL_PREFIX = "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/"


# Schema
INITIAL_COLUMN_TYPES = {
    "date": pa.date32(),
    "serial_number": pa.string(),
    "model": pa.string(),
    "capacity_bytes": pa.int64(),
    "failure": pa.int8(),
    "datacenter": pa.string(),
    "cluster_id": pa.string(),
    "vault_id": pa.string(),
    "pod_id": pa.int8(),
    "pod_slot_num": pa.int8(),
    "is_legacy_format": pa.int8()
}


def drop_null_columns(table):
    """
    Remove any columns that contain only null values. These columns have the
    pa.null() data type.

    :param table: a PyArrow table
    :return: table with no null columns
    """
    null_columns = []
    schema = table.schema
    for (name_, type_) in zip(schema.names, schema.types):
        if type_ == pa.null():
            null_columns.append(name_)
    return table.drop(null_columns)


def write_as_iceberg(table: Table, year: int, month: int, df: pyarrow.Table):
    """
    Write the given PyArrow table via Iceberg

    :param table: Iceberg table to which data will be written
    :param year: Year of the data
    :param month: Month of the data
    :param df: PyArrow table containing data
    """
    print(f'Writing {year}/{month}')
    delay = 1
    for i in range(WRITE_TABLE_RETRIES):
        try:
            table.append(df)
            return
        except OSError as e:
            print(f'Error writing table. Will try again in {delay} seconds. Error was: {str(e)}')
            sleep(delay)
            delay *= 2
    raise OSError(f'Cannot write table after {WRITE_TABLE_RETRIES} tries')


def zipped_csv_to_iceberg(zip_url: str, catalog: Catalog, table: Table | None) -> Table:
    """
    Read Drive Stats data from the given URL and write it to the Iceberg table

    :param catalog: Iceberg catalog
    :param zip_url: URL of a ZIP file containing CSV-formatted Drive Stats data files
    :param table: Iceberg table to which data will be written
    """
    print(f"Getting content from {zip_url}")
    response = requests.get(zip_url)
    response.raise_for_status()

    content = response.content
    myzip = ZipFile(BytesIO(content))

    current_year = None
    current_month = None
    month_so_far_data = None
    for name in sorted(myzip.namelist()):
        match = FILENAME_PATTERN.match(name)
        if match:
            print(f'Reading {name}')
            current_day_data = csv.read_csv(myzip.open(name))

            if current_day_data.num_rows == 0:
                # Some days have no data!
                print(f'Skipping {name} - no data')
                continue

            (year, month, day) = match.groups()

            # Replace the date column to workaround data format issues
            current_day_data = current_day_data.drop(['date'])
            current_day_data = current_day_data.add_column(0, 'date', [[date(int(year), int(month), int(day))] * current_day_data.num_rows])

            # All-null columns in Parquet files cause problems with some tools, so drop them
            current_day_data = drop_null_columns(current_day_data)

            # Set type of all SMART attribute columns to int64, regardless of what PyArrow guessed they were
            column_types = {}
            for column_name in current_day_data.schema.names:
                column_type = INITIAL_COLUMN_TYPES.get(column_name)
                if column_type:
                    column_types[column_name] = column_type
                else:
                    column_types[column_name] = pa.int64()

            current_day_data = current_day_data.cast(target_schema=pa.schema(column_types))

            if table is None:
                table = catalog.create_table(
                    identifier=f"{NAMESPACE}.{TABLE_NAME}",
                    schema=current_day_data.schema,
                    location=f"s3a://{DST_BUCKET}/{TABLE_NAME}",
                )

            if current_year != year or current_month != month:
                # Write out the accumulated data and start a new month
                if month_so_far_data:
                    write_as_iceberg(table, current_year, current_month, month_so_far_data)
                month_so_far_data = current_day_data
                current_year = year
                current_month = month
            else:
                try:
                    # Add this day's data to the month so far
                    month_so_far_data = pa.concat_tables([month_so_far_data, current_day_data])
                except ArrowInvalid as e:
                    if str(e).startswith('Schema at index 1 was different:'):
                        # Schema mismatch: write data for month so far and update schema
                        print('Schema Change!')
                        print(f'Old schema: {len(month_so_far_data.schema.names)} fields', repr(month_so_far_data.schema))
                        print(f'New schema: {len(current_day_data.schema.names)} fields', repr(current_day_data.schema))
                        write_as_iceberg(table, current_year, current_month, month_so_far_data)
                        month_so_far_data = current_day_data
                        with table.update_schema() as update:
                            update.union_by_name(schema)
                    else:
                        raise e
        else:
            print(f'Skipping {name}')
    if month_so_far_data:
        write_as_iceberg(table, current_year, current_month, month_so_far_data)

    return table


def main():
    parser = argparse.ArgumentParser(description='Convert Drive Stats data to Parquet')

    parser.add_argument('quarter', type=int, nargs='?', default=0, help='starting quarter, 1-4')
    parser.add_argument('year', type=int, nargs='?', default=0, help='starting year')
    args = parser.parse_args()

    # Never put credentials in source code!
    load_dotenv(override=True)

    # Extract region from endpoint - you could just configure it separately
    endpoint_pattern = re.compile(r"^https://s3\.([a-zA-Z0-9-]+)\.backblazeb2\.com$")
    region_match = endpoint_pattern.match(os.environ['B2_ENDPOINT'])
    region_name = region_match.group(1)

    warehouse = f"s3a://{DST_BUCKET}/{TABLE_NAME}"

    catalog = SqlCatalog(
        "default",
        **{
            "uri": "sqlite:///:memory:",
            "warehouse": warehouse,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint": os.environ['B2_ENDPOINT'],
            "s3.access-key-id": os.environ['B2_APPLICATION_KEY_ID'],
            "s3.secret-access-key": os.environ['B2_APPLICATION_KEY'],
            "s3.region": region_name
        },
    )

    # TODO - handle existing table!
    catalog.create_namespace(NAMESPACE)
    table = None

    if args.year < FIRST_YEAR_OF_QUARTERLY_DATA:
        # Convert the annual data files
        for year in ANNUAL_DATA:
            if year >= args.year:
                path = f"{DRIVE_STATS_URL_PREFIX}data_{year}.zip"
                table = zipped_csv_to_iceberg(path, catalog, table)

    exit()

    # Convert the quarterly data files
    year = args.year
    quarter = max(args.quarter, 1)
    # Loop until no more data
    while True:
        while quarter <= 4:
            path = f"{DRIVE_STATS_URL_PREFIX}data_Q{quarter}_{year}.zip"
            try:
                table = zipped_csv_to_iceberg(path, catalog, table)
            except requests.HTTPError:
                # We've run out of data
                print(f"No data at {path} - exiting")
                exit(0)
            quarter += 1
        quarter = 1
        year += 1


if __name__ == "__main__":
    main()
