#!/usr/bin/env python3
"""Load influxdb line-protocol data into clickhouse.

This script helps to migrate off influxdb!

Clickhouse password should be set via CH_PASSWORD env var if not blank.

Requirements:
pip3 install line-protocol-parser clickhouse-driver
"""

import argparse
import datetime
import functools
import gzip
import os
import sys

from clickhouse_driver import Client
from line_protocol_parser import LineFormatError, parse_line

DEFAULT_INSERT_SIZE = 10**6
SETTINGS = ['SET max_partitions_per_insert_block=1000']


def filter_measurements(measurements, from_measurement, ignore_measurements):
    """Filter the list of measurements."""
    if ignore_measurements:
        new_list = []
        for m in measurements:
            if m not in ignore_measurements:
                new_list.append(m)

        measurements = new_list

    i = 0
    for m in measurements:
        if m == from_measurement:
            return measurements[i:]

        i += 1

    # Return nothing if from_measurement was given and not matched above.
    if from_measurement:
        return []

    return measurements


def now():
    """Return formatted current time."""
    return datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')


def sanitize_column_name(i):
    """Sanitize column name."""
    return i.replace('-', '_')


def write_records(client, columns, lines, args):
    """Write records into clickhouse."""
    records = {}
    # Parse records.
    for i in lines:
        try:
            data = parse_line(i)
        except LineFormatError as err:
            print(f'LineFormatError: "{err}". Line: {i}')
            continue

        table_name = data['measurement']
        # len(time)==10 for s
        # len(time)==13 for ms
        # len(time)==16 for u
        # len(time)==19 for ns - influx default
        row = {'time': int(data['time'] / 10**(len(str(data['time']))-10) * 10**args.time_precision)}
        row.update(data['tags'])
        row.update(data['fields'])
        # Sanitize column names
        row = dict((sanitize_column_name(k), v) for k, v in row.items())
        row_columns = ','.join(row.keys())

        if table_name not in columns:
            print(f'Skipping 1 row because {table_name} table does not exist.')
            continue

        # Add missing columns.
        for k, v in columns[table_name].items():
            if k in row_columns:
                continue

            v = v.lower()
            if 'string' in v:
                row[k] = ''
            elif 'int' in v or 'float' in v:
                row[k] = 0
            else:
                print(f'Need to set default value for column "{k}" of type "{v}" in the script!')
                sys.exit(-1)

        if table_name not in records:
            records[table_name] = []

        records[table_name].append(row)

    # Write records.
    for table_name, rows in records.items():
        print(f'  {table_name}:{len(rows)}')
        row_columns = '`,`'.join(columns[table_name].keys())
        query = f'INSERT INTO `{table_name}` (`{row_columns}`) VALUES'
        try:
            client.execute(query, rows)
        except KeyError as err:
            print(f'KeyError: {err}')
        except BaseException as err:
            print(err)
            print('Retrying...')
            client.execute(query, rows)


def restore(args):
    """Restore from a backup."""
    password = os.environ.get('CH_PASSWORD', '')
    client = Client(host=args.host, port=args.port, secure=args.secure, user=args.user, password=password, database=args.db)
    for i in SETTINGS:
        client.execute(i)

    if not os.path.exists(args.dir):
        print(f'Backup dir "{args.dir}" does not exist')
        sys.exit(-1)

    measurements = args.measurements
    if measurements:
        measurements = measurements.split(',')

    ignore_measurements = args.ignore_measurements
    if ignore_measurements:
        ignore_measurements = ignore_measurements.split(',')

    if not measurements:
        if args.gzip:
            files = [f[:-3] for f in os.listdir(args.dir) if os.path.isfile(args.dir+'/'+f) and f.endswith('.gz')]
        else:
            files = [f for f in os.listdir(args.dir) if os.path.isfile(args.dir+'/'+f) and not f.endswith('.gz')]

        files.sort()
        measurements = filter_measurements(files, args.from_measurement, ignore_measurements)

    if not measurements:
        print('Nothing to restore. If backup is gzipped, use --gzip option.')
        sys.exit(-1)

    print('Files:')
    print(measurements)
    print()

    if not args.force and input(f'> Confirm restore into "{args.db}" db? [yes/no] ') != 'yes':
        sys.exit(0)

    print()
    # Get list of columns for each table.
    data = client.execute(f"SELECT table, name, type FROM system.columns WHERE database='{args.db}'")
    columns = {}
    for x in data:
        if x[0] not in columns:
            columns[x[0]] = {}

        columns[x[0]][x[1]] = x[2]

    # Iterate over files.
    for m in measurements:
        print(f'Loading {m}... ', end='')
        if args.mixed_files:
            print()

        if not args.mixed_files and m not in columns:
            print('Skipping because the corresponding table does not exist.')
            continue

        lines = []
        line_count = 0
        if args.gzip:
            f = gzip.open(f'{args.dir}/{m}.gz', 'rt')
        else:
            f = open(f'{args.dir}/{m}', 'r')

        for i in f:
            if len(lines) == args.insert_size:
                write_records(client, columns, lines, args)
                lines = []
                line_count += args.insert_size

            lines.append(i)

        if lines:
            write_records(client, columns, lines, args)

        print(line_count+len(lines))
        f.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load influxdb line-protocol backup into clickhouse')
    parser.add_argument('--host', help='Clickhouse host', default='localhost')
    parser.add_argument('--port', help='Clickhouse port', default=9000)
    parser.add_argument('--secure', help='Clickhouse secure connection', action='store_true')
    parser.add_argument('--user', help='Clickhouse user. The password should be set via CH_PASSWORD env var if not blank.', default='default')
    parser.add_argument('--dir', required=True, help='directory with the backup to restore form')
    parser.add_argument('--db', required=True, help='Clickhouse database to restore into')
    parser.add_argument('--measurements', help='comma-separated list of measurements to restore')
    parser.add_argument('--ignore-measurements', help='comma-separated list of measurements to skip from restore (ignored when using --measurements)')
    parser.add_argument('--from-measurement', help='restore starting from this measurement and on (ignored when using --measurements)')
    parser.add_argument('--gzip', action='store_true', help='restore from gzipped files')
    parser.add_argument('--insert-size', help='number of records to insert with a single statement', default=DEFAULT_INSERT_SIZE)
    parser.add_argument('--time-precision', help='time precision to store, corresponds to the type of "time" column. Default 3, i.e. ms', default=3)
    parser.add_argument('--mixed-files', action='store_true', help='backup files contain mixed measurements, file names are not table names etc.')
    parser.add_argument('--force', action='store_true', help='do not ask for confirmation')
    args = parser.parse_args()

    if 'REQUESTS_CA_BUNDLE' not in os.environ:
        os.environ['REQUESTS_CA_BUNDLE'] = '/etc/ssl/certs/ca-certificates.crt'

    # Enable unbuffered output.
    print = functools.partial(print, flush=True)

    print(f'<< {now()}')
    print(f'Starting restore from "{args.dir}" dir to "{args.db}" db.\n')
    restore(args)
    print('Done.')
    print(f'<< {now()}')
