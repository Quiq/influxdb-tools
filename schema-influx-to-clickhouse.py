#!/usr/bin/env python3
"""Generate table schemas for Clickhouse based on influxdb measurements.

This script helps to migrate off influxdb!

Influxdb passwod should be set via INFLUX_PASSWORD env var.
"""

import argparse
import functools
import json
import os
import pprint
import sys

import requests


def query_influxdb(args, params):
    """Run query on influxdb."""
    password = os.environ.get('INFLUX_PASSWORD')
    if not password:
        print('INFLUX_PASSWORD env var has to be set!')
        sys.exit(1)

    r = requests.get(f'{args.url}/query', auth=(args.user, password), params=params)
    if r.status_code != 200:
        sys.exit(-1)

    data = json.loads(r.text)
    return data


def get_measurements(args):
    """Get measurements and their properties."""
    # Get measurements.
    data = query_influxdb(args, {'q': 'SHOW MEASUREMENTS', 'db': args.db})
    if 'series' in data['results'][0]:
        measurements = [i[0] for i in data['results'][0]['series'][0]['values']]

    if args.verbose:
        pprint.pprint(measurements)

    # Get fields from default retention.
    queries = []
    for m in measurements:
        queries.append(f'SHOW FIELD KEYS FROM "{m}"')

    data = query_influxdb(args, params={'q': ';'.join(queries), 'db': args.db})
    mstagfields = {}
    for i in data['results']:
        # Empty measurement has no fields or a measurement exists only in non-default retention.
        # Skip such measurements.
        if 'series' not in i:
            continue

        mstagfields[i['series'][0]['name']] = {x[0]: x[1] for x in i['series'][0]['values']}

    # Get tags.
    queries = []
    for m in mstagfields:
        queries.append(f'SHOW TAG KEYS FROM "{m}"')

    data = query_influxdb(args, params={'q': ';'.join(queries), 'db': args.db})
    for i in data['results']:
        if 'series' not in i:
            continue

        mstagfields[i['series'][0]['name']].update({x[0]: 'tag' for x in i['series'][0]['values']})

    if args.verbose:
        pprint.pprint(mstagfields)

    return mstagfields


def sanitize_column_name(i):
    """Sanitize column name."""
    return i.replace('-', '_')


def generate_schemas(args, mstagfields):
    """Generate table schemas."""
    for table, tagfields in mstagfields.items():
        columns = []
        primary_key = []
        for k, v in tagfields.items():
            k = sanitize_column_name(k)
            if v == 'integer':
                columns.append(f'`{k}` Int64')
            elif v == 'float':
                columns.append(f'`{k}` Float32')
            elif v == 'string':
                columns.append(f'`{k}` String')
            elif v == 'tag':
                columns.append(f'`{k}` LowCardinality(String)')
                primary_key.append(f'`{k}`')
            else:
                print(f'Unknown type on {table}: {k} {v}')
                sys.exit(-1)

        columns = ',\n                '.join(columns)
        columns = columns + ','
        primary_key = ', '.join(primary_key)
        query = f'''
            CREATE TABLE `{table}` (
                {columns}
                `time` {args.time_type} CODEC(DoubleDelta)
            ) ENGINE = {args.engine}
            PARTITION BY {args.partition_by}
            ORDER BY ({primary_key}, time);
        '''
        print(query)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate table schemas for Clickhouse based on influxdb measurements')
    parser.add_argument('--url', required=True, help='Influxdb url')
    parser.add_argument('--user', help='Influxdb user. The password should be set via INFLUX_PASSWORD env var.', default='admin')
    parser.add_argument('--db', required=True, help='Influxdb database to get measurements from')
    parser.add_argument('--engine', help='Clickhouse table engine to define', default='ReplacingMergeTree')
    parser.add_argument('--partition-by', help='Clickhouse table PARTITION BY definition', default='toYYYYMM(time)')
    parser.add_argument('--time-type', help='type of "time" column', default='DateTime')
    parser.add_argument('--verbose', action='store_true', help='verbose mode')
    args = parser.parse_args()

    if 'REQUESTS_CA_BUNDLE' not in os.environ:
        os.environ['REQUESTS_CA_BUNDLE'] = '/etc/ssl/certs/ca-certificates.crt'

    # Enable unbuffered output.
    print = functools.partial(print, flush=True)

    mstagfields = get_measurements(args)
    generate_schemas(args, mstagfields)
