import argparse
import logging
import os
import re
import sys
from datetime import date
from io import BytesIO
from time import sleep
from zipfile import ZipFile

import pyarrow as pa
import requests
from dotenv import load_dotenv
from pyarrow import compute, csv, fs
from pyiceberg.catalog import LOCATION, load_catalog
from pyiceberg.exceptions import NoSuchNamespaceError
from pyiceberg.io.pyarrow import pyarrow_to_schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.table import Table
from pyiceberg.table.name_mapping import MappedField, NameMapping
from pyiceberg.transforms import MonthTransform

from timestamp_month import TimestampMonth

logger = logging.getLogger(__name__)

# Number of times to retry the write_table operation
WRITE_TABLE_RETRIES = 8

# Match the CSV filenames within the zip files
# Various forms:
# 2014/2014-06-25.csv
# data_Q1_2016/2016-03-08.csv
# 2017-03-01.csv
CSV_FILENAME_PATTERN = re.compile(r"^(?:.*/)?(\d\d\d\d)-(\d\d)-(\d\d).csv$")

# First three years had annual data
ANNUAL_DATA = [2013, 2014, 2015]
FIRST_YEAR_OF_ANNUAL_DATA = ANNUAL_DATA[0]

# Quarterly data starts Q1 2016
FIRST_YEAR_OF_QUARTERLY_DATA = 2016
# Path to Drives Stats files
DRIVE_STATS_URL_PREFIX = "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/"

# Base Schema without smart attributes
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

# URL scheme used with PyIceberg etc. Can change this to another value if necessary
FS_SCHEME = "s3"

DEFAULT_NAMESPACE = 'default'
DEFAULT_TABLE_NAME = 'drivestats'
DEFAULT_CONNECT_TIMEOUT = 5
DEFAULT_REQUEST_TIMEOUT = 20

def get_column_type(column_name):
    """
    Given a column name, return the correct type
    """
    column_type = INITIAL_COLUMN_TYPES.get(column_name)
    if column_type:
        return column_type
    elif column_name.startswith('smart_'):
        return pa.int64()
    else:
        raise ValueError(f'Unexpected column name: {column_name}')


def get_normalized_schema(table: Table):
    """
    Given an Iceberg table, return its schema, using our preferred data types
    """
    fields = []
    for column_name in table.schema().column_names:
        fields.append((column_name, get_column_type(column_name)))
    return pa.schema(fields)


def get_iceberg_schema(table: pa.Table):
    """
    From https://stackoverflow.com/a/78946023/33905
    Create mapping from column names to unique integers
    """
    field_id = 0
    array = []
    for name in table.column_names:
        field_id += 1
        array.append(MappedField(field_id=field_id, names=[name]))  # noqa - bogus unexpected argument warning
    return pyarrow_to_schema(table.schema, NameMapping(array), downcast_ns_timestamp_to_us=True)  # noqa - bogus unexpected argument warning


def get_max_year_month(table: Table) -> (int, int):
    """
    Given an Iceberg table, return the latest year, month in the data, based on the table's partitions
    """
    partitions = table.inspect.partitions()
    partition_field_index = partitions.column_names.index('partition')
    max_partition_value = 0
    for partition in partitions.columns[partition_field_index]:
        max_partition_value = max(max_partition_value, partition.get('date_month').as_py())
    max_year = (max_partition_value // 12) + 1970
    max_month = max_partition_value % 12  # Zero-based!
    return max_year, max_month


def get_region(endpoint: str):
    """
    Extract region from endpoint - you could just configure it separately
    """
    endpoint_pattern = re.compile(r'^https://s3\.([a-zA-Z0-9-]+)\.backblazeb2\.com$')
    region_match = endpoint_pattern.match(endpoint)
    region_name = region_match.group(1)
    return region_name


def schema_equals_ignore_order(schema1: pa.Schema, schema2: pa.Schema):
    """
    Default pa.schema.equals considers field order, but we don't care about it
    """
    return set(schema1.names) == set(schema2.names) and all(
        schema1.field(name).type == schema2.field(name).type for name in schema1.names
    )


SUFFIXES = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
def human_readable(num_bytes: int):
    i = 0
    while num_bytes >= 1000 and i < len(SUFFIXES)-1:
        num_bytes /= 1000.
        i += 1
    f = ('%.2f' % num_bytes).rstrip('0').rstrip('.')
    return '%s %s' % (f, SUFFIXES[i])


class Processor:
    def __init__(self, bucket: str, namespace: str, table_name: str, connect_timeout: float=DEFAULT_CONNECT_TIMEOUT, request_timeout: float=DEFAULT_REQUEST_TIMEOUT):
        self.bucket = bucket
        self.namespace = namespace
        self.table_name = table_name
        self.table = None
        self.schema = None
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout

        self._create_catalog()
        self._create_namespace_if_not_exist()

        metadata_location = self.get_metadata_location()
        if metadata_location is None:
            logger.info(f'No Iceberg table metadata found at "{self.bucket}/{self.table_name}/metadata/"')
            self.max_year = FIRST_YEAR_OF_ANNUAL_DATA
            self.max_month = 0
        else:
            logger.info(f'Found "{self.table_name}" table at "{self.bucket}/{self.table_name}/"')
            logger.info(f'Current metadata location is {metadata_location}')
            self.table = self.catalog.register_table(self.get_table_identifier(), metadata_location)
            self.log_table_stats()
            self.schema = get_normalized_schema(self.table)
            self.max_year, self.max_month = get_max_year_month(self.table)
            logger.info(f'Latest month in data is {self.max_month + 1}/{self.max_year}')

    def _create_catalog(self):
        """
        # Create an in-memory catalog so we can create a new Iceberg table or register an existing one
        """
        self.catalog = load_catalog(
            'iceberg',
            **{
                'uri': 'sqlite:///:memory:',
                # The underlying AWS SDK for C++ does not support AWS_ENDPOINT_URL
                # and AWS_REGION environment variables (see
                # https://github.com/aws/aws-sdk-cpp/issues/2587) so we have to pass
                # them explicitly
                's3.endpoint': os.environ['AWS_ENDPOINT_URL'],
                's3.region': get_region(os.environ['AWS_ENDPOINT_URL']),
                's3.request-timeout': self.request_timeout,
                's3.connect-timeout': self.connect_timeout,
            }
        )

    def _create_namespace_if_not_exist(self):
        try:
            self.catalog.list_namespaces(namespace=self.namespace)
        except NoSuchNamespaceError:
            self.catalog.create_namespace(self.namespace, { LOCATION: f'{FS_SCHEME}://{self.bucket}/'})

    def get_table_identifier(self):
        return f'{self.namespace}.{self.table_name}'

    def create_table(self, pa_table: pa.Table):
        """
        Create the Iceberg table based on the Iceberg schema of a given PyArrow table
        """
        iceberg_schema = get_iceberg_schema(pa_table)
        partition_field = iceberg_schema.find_field('date')
        self.table = self.catalog.create_table(
            identifier=self.get_table_identifier(),
            schema=iceberg_schema,
            partition_spec=PartitionSpec(
                PartitionField(
                    source_id=partition_field.field_id,
                    field_id=partition_field.field_id,
                    transform=MonthTransform(),
                    name="date_month"
                )
            )
        )

    def update_schema(self, current_data):
        self.log_schema_change(current_data)
        self.schema = current_data.schema
        with self.table.update_schema() as update:
            update.union_by_name(self.schema)

    def process_file(self, zip_url: str, starting_month: TimestampMonth):
        """
        Read Drive Stats data from the given URL and write it to the Iceberg table

        :param zip_url: URL of a ZIP file containing CSV-formatted Drive Stats data files
        :param starting_month: Zero-based month number to start from
        """
        logger.info(f"Reading data from {zip_url}")
        zip_file = self.get_zip_file(zip_url)

        # Read each day's data from CSV, accumulating up to a month's data at a time, writing to the table at the end
        # of the month or when the schema changes, whichever is sooner.
        year = None
        month = None
        month_data = None
        for name in sorted(zip_file.namelist()):
            match = CSV_FILENAME_PATTERN.match(name)
            if not match:
                logger.debug(f'Skipping {name} (no match)')
                continue

            # Extract date components from CSV filename
            (current_year, current_month, current_day) = match.groups()

            if TimestampMonth.from_year_month(int(current_year), int(current_month) - 1) < starting_month:
                logger.debug(f'Skipping {name} (before start)')
                continue

            logger.info(f'Reading {name}')
            current_data = csv.read_csv(zip_file.open(name))

            if current_data.num_rows == 0:
                logger.info(f'Skipping {name} - no data')
                continue

            # Do some basic data cleaning
            current_data = self.massage_data(current_data, int(current_year), int(current_month), int(current_day))

            if self.table is None:
                logger.info(f'Creating table {self.table_name}.')
                self.schema = current_data.schema
                self.create_table(current_data)

            if not schema_equals_ignore_order(current_data.schema, self.schema):
                # Schema mismatch: write data for month so far then update schema.
                self.write_data(year, month, month_data)
                month_data = current_data

                self.update_schema(current_data)
            elif year != current_year or month != current_month:
                # We hit a new month. Write out the accumulated data.
                self.write_data(year, month, month_data)
                month_data = current_data

                year = current_year
                month = current_month
            else:
                # Add the current day's data to the month so far.
                month_data = pa.concat_tables([month_data, current_data])

        # Write remaining data
        if month_data:
            self.write_as_iceberg(year, month, month_data)

    def log_schema_change(self, current_data):
        logger.info('Schema Change!')
        if logger.getEffectiveLevel() <= logging.DEBUG:
            logger.debug(f'Old schema: {len(self.schema.names)} fields: '
                         f'{self.schema.to_string(truncate_metadata=False).replace('\n', ', ')}')
            logger.debug(f'New schema: {len(current_data.schema.names)} fields: '
                         f'{current_data.schema.to_string(truncate_metadata=False).replace('\n', ', ')}')
        else:
            logger.info(f'Old schema: {len(self.schema.names)} fields. '
                        f'New schema: {len(current_data.schema.names)} fields')

    def write_data(self, year, month, month_data):
        if month_data and month_data.num_rows > 0:
            self.write_as_iceberg(year, month, month_data)

    @staticmethod
    def get_zip_file(zip_url: str) -> ZipFile:
        response = requests.get(zip_url)
        response.raise_for_status()
        zip_file = ZipFile(BytesIO(response.content))
        return zip_file

    @staticmethod
    def drop_null_columns(table: pa.Table):
        """
        Remove any columns that contain only null values.
        """
        null_columns = []
        schema = table.schema
        for (name_, type_) in zip(schema.names, schema.types):
            if type_ == pa.null():
                null_columns.append(name_)
        return table.drop(null_columns)

    @staticmethod
    def normalize_column_types(data):
        """
        Set types of all SMART attribute columns to int64, regardless of what PyArrow guessed they were
        """
        column_types = {}
        for column_name in data.schema.names:
            column_types[column_name] = get_column_type(column_name)
        # Apply normalized column types to data
        return data.cast(target_schema=pa.schema(column_types))

    def write_as_iceberg(self, year: int, month: int, table: pa.Table):
        """
        Write the given PyArrow table via Iceberg
        """
        logger.info(f'Writing {table.num_rows} rows for {month}/{year} to {".".join(self.table.name())}')
        delay = 1
        for i in range(WRITE_TABLE_RETRIES):
            try:
                self.table.append(table)
                return
            except OSError as e:
                logger.warning(f'Error writing table. Will try again in {delay} second(s). Error was: {str(e)}')
                sleep(delay)
                delay *= 2
        raise OSError(f'Cannot write table after {WRITE_TABLE_RETRIES} tries')

    def massage_data(self, data: pa.Table, year: int, month: int, day: int) -> pa.Table:
        """
        Replace the date column to workaround data format issues. Drop any all-null columns to avoid problems with some
        tools. Normalize SMART attribute column types to int64.
        """
        data = (
            data.drop(['date'])
            .add_column(
                0,
                'date',
                [[date(year, month, day)] * data.num_rows]
            )
        )
        return self.normalize_column_types(self.drop_null_columns(data))

    def get_metadata_location(self) -> str | None:
        s3fs = fs.S3FileSystem(
            # Explicitly set endpoint and region since the underlying AWS SDK
            # for C++ does not support the environment variables (see
            # https://github.com/aws/aws-sdk-cpp/issues/2587)
            endpoint_override=os.environ['AWS_ENDPOINT_URL'],
            region=get_region(os.environ['AWS_ENDPOINT_URL']),
            request_timeout=self.request_timeout,
            connect_timeout=self.connect_timeout,
        )

        # List files at the metadata location
        prefix = f"{self.bucket}/{self.table_name}/metadata/"
        files = s3fs.get_file_info(fs.FileSelector(prefix, True, True))

        # Metadata files have suffix '.metadata.json' and are named sequentially with numeric prefixes,
        # so we can simply filter the listing, sort it, and take the last element
        if len(metadata_locations := sorted([file.path for file in files if file.path.endswith('.metadata.json')])) > 0:
           return f'{FS_SCHEME}://{metadata_locations[-1]}'

        return None

    def log_table_stats(self):
        if logger.isEnabledFor(logging.INFO):
            files = self.table.inspect.files()
            total_bytes = compute.sum(files['file_size_in_bytes'])  # noqa - bogus cannot find reference warning
            total_records = compute.sum(files['record_count'])  # noqa - bogus cannot find reference warning
            file_count = files.num_rows
            logger.info(f'"{self.table_name}" table at {self.table.location()} has {total_records} rows, {file_count} files and occupies {human_readable(total_bytes.as_py())}')

    def log_stats_and_exit(self, path):
        # We've run out of data
        logger.info(f"No data at {path} - exiting")
        self.log_table_stats()
        sys.exit(0)


def check_quarter(value):
    error_format = "%s is an invalid quarter value; quarter must be between 1 and 4."
    try:
        int_value = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(error_format % value)
    if int_value < 1 or int_value > 4:
        raise argparse.ArgumentTypeError(error_format % int_value)
    return int_value


def check_year(value):
    error_format = "%s is an invalid year value; year must be at 2013 or later."
    try:
        int_value = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(error_format % value)
    if int_value < FIRST_YEAR_OF_ANNUAL_DATA:
        raise argparse.ArgumentTypeError(error_format % int_value)
    return int_value


def parse_arguments():
    parser = argparse.ArgumentParser(description='Convert Drive Stats data to Parquet.')

    parser.add_argument('bucket', metavar='bucket-name', type=str, help='Destination bucket name')
    parser.add_argument('namespace', type=str, nargs='?',
                        default=DEFAULT_NAMESPACE, help=f'Destination namespace; defaults to {DEFAULT_NAMESPACE}')
    parser.add_argument('table_name', metavar='table-name', type=str, nargs='?',
                        default=DEFAULT_TABLE_NAME, help=f'Destination table name; defaults to {DEFAULT_TABLE_NAME}')
    parser.add_argument('quarter', type=check_quarter, nargs='?', help='Starting quarter, 1-4')
    parser.add_argument('year', type=check_year, nargs='?', help='Starting year')

    parser.add_argument('--log', type=str, nargs='?', default='INFO')
    parser.add_argument('--debug-pyarrow-fs', action='store_true',
                        help='Set PyArrow log level to "Debug"; generates LOTS of output!')
    parser.add_argument('--connect-timeout', type=float, default=DEFAULT_CONNECT_TIMEOUT,
                        help=f'Connect timeout, in seconds. Defaults to {DEFAULT_CONNECT_TIMEOUT}.')
    parser.add_argument('--request-timeout', type=float, default=DEFAULT_REQUEST_TIMEOUT,
                        help=f'Request timeout, in seconds. Defaults to {DEFAULT_REQUEST_TIMEOUT}.')
    parser.add_argument('--check-only', action='store_true', help='Display table metadata only; do not write any data.')

    return parser.parse_args()


def main():
    args = parse_arguments()

    # Check that the supplied log level is valid
    log_level = getattr(logging, args.log.upper(), None)
    if not isinstance(log_level, int):
        raise ValueError('Invalid log level: %s' % args.log)

    # Basic logging setup - log to stderr with the supplied log level
    logging.basicConfig()
    logger.setLevel(level=log_level)

    # Optionally enable PyArrow logging
    if args.debug_pyarrow_fs:
        logger.info('Setting PyArrow log level to "Debug"')
        fs.initialize_s3(fs.S3LogLevel.Debug)

    if bool(args.quarter) != bool(args.year):
        logger.error("You must specify both quarter and year or neither")
        sys.exit(1)

    # Never put credentials in source code!
    load_dotenv(override=True)

    # The Processor constructor logs table metadata, so call it even if we are just checking the table metadata
    processor = Processor(args.bucket, args.namespace, args.table_name,
                          connect_timeout=args.connect_timeout,
                          request_timeout=args.request_timeout)

    if args.check_only:
        sys.exit(0)

    if args.year is not None and args.quarter is not None:
        starting_year = args.year
        starting_quarter = args.quarter - 1
        starting_month = TimestampMonth.from_year_quarter(args.year, starting_quarter)
    else:
        # Start from month following the last month that we have
        # TimestampMonth handles all the logic around crossing quarter/year boundaries
        starting_month = TimestampMonth.from_year_month(processor.max_year, processor.max_month) + 1
        starting_year = starting_month.year()
        starting_quarter = starting_month.quarter()

    # Sanity checks
    assert starting_month >= TimestampMonth.from_year_month(FIRST_YEAR_OF_ANNUAL_DATA, 0), 'Starting month out of bounds'
    assert starting_quarter in range(0, 4), 'Starting quarter out of bounds'
    assert starting_year >= FIRST_YEAR_OF_ANNUAL_DATA, 'Starting year out of bounds'

    # Convert the annual data files
    for year in range(starting_year, FIRST_YEAR_OF_QUARTERLY_DATA):
        path = f"{DRIVE_STATS_URL_PREFIX}data_{year}.zip"
        try:
            processor.process_file(path, starting_month)
        except requests.HTTPError:
            processor.log_stats_and_exit(path)

    # Convert the quarterly data files
    year = max(starting_year, FIRST_YEAR_OF_QUARTERLY_DATA)
    quarter = starting_quarter

    while True:
        while quarter < 4:
            path = f"{DRIVE_STATS_URL_PREFIX}data_Q{quarter + 1}_{year}.zip"
            try:
                processor.process_file(path, starting_month)
            except requests.HTTPError:
                processor.log_stats_and_exit(path)
            quarter += 1
        # Start a new year
        quarter = 0
        year += 1


if __name__ == "__main__":
    main()
