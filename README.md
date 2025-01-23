# Drive Stats to Iceberg

This application converts the Backblaze Drive Stats data set from zipped CSV files to Apache Parquet files with the Apache
Iceberg table format.

## Configuration

Copy `.env.template` to `.env` and set the environment variables:

```dotenv
AWS_ACCESS_KEY_ID=<your Backblaze application key ID>
AWS_SECRET_ACCESS_KEY=<your Backblaze application key>
AWS_ENDPOINT_URL=https://<your Backblaze bucket's endpoint>
```

## Usage

You must specify the destination bucket when running the application. You may also specify the namespace and table-name, 
which default to `default` and `drivestats` respectively; and the quarter and year from which to start processing data; 
you must specify both quarter and year or neither. If you do not specify quarter and year then processing starts with
the data from 2013, the first year that Backblaze published the Drive Stats data set.

There are also several options to set log level, timeouts and to display the table metadata without writing any data:

```console
% python drivestats2iceberg.py --help
usage: drivestats2iceberg.py [-h] [--log [LOG]] [--debug-pyarrow-fs] [--connect-timeout CONNECT_TIMEOUT] [--request-timeout REQUEST_TIMEOUT] [--check-only] bucket-name [namespace] [table-name] [quarter] [year]

Convert Drive Stats data to Parquet.

positional arguments:
  bucket-name           Destination bucket name
  namespace             Destination namespace; defaults to default
  table-name            Destination table name; defaults to drivestats
  quarter               Starting quarter, 1-4
  year                  Starting year

options:
  -h, --help            show this help message and exit
  --log [LOG]
  --debug-pyarrow-fs    Set PyArrow log level to "Debug"; generates LOTS of output!
  --connect-timeout CONNECT_TIMEOUT
                        Connect timeout, in seconds. Defaults to 5.
  --request-timeout REQUEST_TIMEOUT
                        Request timeout, in seconds. Defaults to 20.
  --check-only          Display table metadata only; do not write any data.
```

The destination table name is used as a path prefix for the files in the Backblaze B2 Bucket.

### Usage Examples

Creating the Iceberg table from scratch:

```console
% python drivestats2iceberg.py metadaddy-drivestats 
INFO:__main__:No Iceberg table metadata found at "metadaddy-drivestats/drivestats/metadata/"
INFO:__main__:Reading data from https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_2013.zip
INFO:__main__:Reading 2013/2013-04-10.csv
INFO:__main__:Creating table drivestats.
INFO:__main__:Reading 2013/2013-04-11.csv
...
INFO:__main__:Reading 2013/2013-05-01.csv
INFO:__main__:Writing 452821 rows for 04/2013 to default.drivestats
...
INFO:__main__:"drivestats" table at s3://drivestats-iceberg/drivestats has 6760937 rows, 17 files and occupies 65.39 MB
```

Appending data to an existing Iceberg table:

```console
python drivestats2iceberg.py metadaddy-drivestats 
INFO:__main__:Found "drivestats" table at "metadaddy-drivestats/drivestats/"
INFO:__main__:Current metadata location is s3://metadaddy-drivestats/drivestats/metadata/00021-8da319ed-91d4-4477-9ddf-cc3f7617a7d5.metadata.json
INFO:__main__:"drivestats" table at s3://metadaddy-drivestats/drivestats has 6760937 rows, 17 files and occupies 65.39 MB
INFO:__main__:Latest month in data is 2/2014
INFO:__main__:Reading data from https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_2014.zip
INFO:__main__:Reading 2014/2014-03-01.csv
...
INFO:__main__:Reading 2014/2014-03-16.csv
INFO:__main__:Writing 454951 rows for 03/2014 to default.drivestats
...
INFO:__main__:Reading data from https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q1_2025.zip
INFO:__main__:No data at https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q1_2025.zip - exiting
INFO:__main__:"drivestats" table at s3://drivestats-iceberg/drivestats has 564566016 rows, 143 files and occupies 19.81 GB
```
