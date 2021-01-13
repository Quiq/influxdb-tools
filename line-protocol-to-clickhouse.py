#!/usr/bin/env python3
"""Load influxdb line-protocol data into clickhouse.

This script helps to migrate off influxdb!

Clickhouse password can be set via CH_PASSWORD env var.

Requirements:
pip3 install line-protocol-parser clickhouse-driver
"""

import argparse
import datetime
import functools
import gzip
import os
import sys
import time

from clickhouse_driver import Client
from line_protocol_parser import parse_line

DEFAULT_ENGINE = 'MergeTree'
DEFAULT_CHUNK_SIZE = 100000
AUTO_SCHEMA = {}


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


def sanitize_column_name(i):
    """Sanitize column name."""
    return i.replace('-', '_')


def manage_schema(client, data, args):
    """Create/drop table if requested."""
    measurement = data['measurement']
    if AUTO_SCHEMA.get(measurement):
        return

    if args.auto_drop_schema:
        query = f'''
            DROP TABLE IF EXISTS `{measurement}`'''
        print(query)
        client.execute(query)

    if args.auto_create_schema:
        columns = ''
        for i in data['tags'].keys():
            i = sanitize_column_name(i)
            columns = columns + f'`{i}` String, '

        for k, v in data['fields'].items():
            k = sanitize_column_name(k)
            if type(v) == int:
                columns = columns + f'`{k}` Int64, '
            elif type(v) == float:
                columns = columns + f'`{k}` Float64, '
            elif type(v) == bool:
                columns = columns + f'`{k}` Boolean, '
            elif type(v) == str:
                columns = columns + f'`{k}` String, '
            else:
                print('Unknown type: ', k, v)
                sys.exit(-1)

        query = f'''
            CREATE TABLE IF NOT EXISTS `{measurement}` (
                {columns}
                `time` DateTime64(0) CODEC(DoubleDelta)
            ) ENGINE = {DEFAULT_ENGINE} ORDER BY (time)
        '''
        print(query)
        client.execute(query)

    AUTO_SCHEMA[measurement] = True


def write_records(client, lines, args):
    """Write records into clickhouse."""
    if args.chunk_delay:
        time.sleep(float(args.chunk_delay))

    records = []
    for i in lines:
        data = parse_line(i)
        time_str = str(data['time'])
        # len(time)==10 for s
        # len(time)==13 for ms
        # len(time)==16 for u
        # len(time)==19 for ns - influx default
        # Assuming `time` DateTime64(0) where 0 is subsec precision which is s
        row = {'time': int(data['time'] / 10**(len(time_str)-10))}
        row.update(data['tags'])
        row.update(data['fields'])
        # Sanitize column names
        row = dict((sanitize_column_name(k), v) for k, v in row.items())
        records.append(row)

    # Drop/create table if requested.
    manage_schema(client, data, args)

    # Write records
    columns = ','.join(row.keys())
    client.execute(f'INSERT INTO `{data["measurement"]}` ({columns}) VALUES', records)


def restore(args):
    """Restore from a backup."""
    password = os.environ.get('CH_PASSWORD', '')
    client = Client(host=args.host, user=args.user, password=password, database=args.db)

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
    for m in measurements:
        if m != measurements[0] and args.measurement_delay:
            time.sleep(float(args.measurement_delay))

        print(f'Loading {m}... ', end='')
        lines = []
        line_count = 0
        if args.gzip:
            f = gzip.open(f'{args.dir}/{m}.gz', 'rt')
        else:
            f = open(f'{args.dir}/{m}', 'r')

        for i in f:
            if len(lines) == args.chunk_size:
                write_records(client, lines, args)
                lines = []
                line_count += args.chunk_size

            lines.append(i)

        if lines:
            write_records(client, lines, args)

        print(line_count+len(lines))
        f.close()


def now():
    """Return formatted current time."""
    return datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load influxdb line-protocol backup into clickhouse')
    parser.add_argument('--host', help='Clickhouse host', default='localhost')
    parser.add_argument('--user', help='Clickhouse user. The password can be set via CH_PASSWORD env var.', default='default')
    parser.add_argument('--dir', required=True, help='directory with the backup to restore form')
    parser.add_argument('--db', required=True, help='Clickhouse database to restore into')
    parser.add_argument('--measurements', help='comma-separated list of measurements to restore')
    parser.add_argument('--ignore-measurements', help='comma-separated list of measurements to skip from restore (ignored when using --measurements)')
    parser.add_argument('--from-measurement', help='restore starting from this measurement and on (ignored when using --measurements)')
    parser.add_argument('--gzip', action='store_true', help='restore from gzipped files')
    parser.add_argument('--chunk-size', help='number of records to insert with a single statement', default=DEFAULT_CHUNK_SIZE)
    parser.add_argument('--chunk-delay', help='restore delay in sec or subsec between chunks')
    parser.add_argument('--measurement-delay', help='restore delay in sec or subsec between measurements')
    parser.add_argument('--force', action='store_true', help='do not ask for confirmation')
    parser.add_argument('--auto-create-schema', action='store_true', help='create table if not exists from the first measurement record info')
    parser.add_argument('--auto-drop-schema', action='store_true', help='drop table if exists before restore')
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
