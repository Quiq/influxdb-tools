#!/usr/bin/env python3
"""InfluxDB backup/restore script using HTTP API and line-protocol format.

Requires python 3.6+
"""

import argparse
import datetime
import functools
import getpass
import gzip
import json
import os
import sys
import time

import requests

READ_CHUCK_SIZE = 10000
WRITE_CHUNK_SIZE = 5000


def measurement2filename(m):
    return m.replace('/', '\u2571')


def filename2measurement(d):
    return d.replace('\u2571', '/')


def identifier2lineprotocol(d):
    return d.replace(' ', r'\ ').replace(',', r'\,').replace('=', r'\=')


def query_influxdb(params):
    """Run query on influxdb."""
    r = requests.get(URL+'/query', auth=AUTH, params=params)
    if r.status_code != 200:
        print(params)
        print(r.status_code, r.text)
        sys.exit(-1)

    data = json.loads(r.text)
    return data


def filter_measurements(measurements):
    """Filter measurement list."""
    if IGNORE_MEASUREMENTS:
        new_list = []
        for m in measurements:
            if m not in IGNORE_MEASUREMENTS:
                new_list.append(m)

        measurements = new_list

    i = 0
    for m in measurements:
        if m == FROM_MEASUREMENT:
            return measurements[i:]

        i += 1

    # Return nothing if FROM_MEASUREMENT was given and not matched above.
    if FROM_MEASUREMENT:
        return []

    return measurements


def chunked_read(db, query):
    """Chunked request to InfluxDB."""
    r = requests.get(URL+'/query', auth=AUTH, stream=True,
                     params={'q': query, 'db': db, 'epoch': 'ns', 'chunked': 'true', 'chunk_size': READ_CHUCK_SIZE})
    if r.status_code != 200:
        print(r.status_code, r.text)
        sys.exit(-1)

    # r.iter_lines() is an iterator where 1 line contains 1 chunk of data coming from InfluxDB.
    return r.iter_lines()


def format_rows(m, msfields, data):
    """Parse response from InfluxDB and format rows to write into the backup file."""
    rows = []
    for a in data['results']:
        if 'series' not in a:
            break

        for b in a['series']:
            for c in b['values']:
                timestamp = 0
                tags = []
                fields = []
                for i in range(len(b['columns'])):
                    col = b['columns'][i]
                    val = c[i]
                    if val is None or val == '':
                        continue

                    if col == 'time':
                        timestamp = val
                    elif col in msfields.keys():
                        # Add double-quotes only for strings.
                        if msfields[col] == 'string':
                            val = val.replace('"', '\\"')
                            val = f'"{val}"'
                        elif msfields[col] == 'integer':
                            val = f'{val}i'

                        fields.append(f'{identifier2lineprotocol(col)}={val}')
                    else:
                        if type(val) == str:
                            val = identifier2lineprotocol(val)

                        tags.append(f'{identifier2lineprotocol(col)}={val}')

                if timestamp == 0 or len(fields) == 0:
                    print(f'No "time" column or 0 fields for "{m}": time {timestamp}, fields {fields}')
                    sys.exit(-1)

                # Format: agent_status,agent=foo\ bar,tenant=roman duration_in_old_status=1207920,new_status="offline",old_status="available" 1496310265009000000
                if tags:
                    rows.append(f"{identifier2lineprotocol(m)},{','.join(tags)} {','.join(fields)} {timestamp}\n")
                else:
                    rows.append(f"{identifier2lineprotocol(m)} {','.join(fields)} {timestamp}\n")

    return rows


def dump(db, where):
    """Create a backup."""
    measurements = MEASUREMENTS
    if not measurements:
        data = query_influxdb({'q': 'SHOW MEASUREMENTS', 'db': db})
        if 'series' in data['results'][0]:
            measurements = [i[0] for i in data['results'][0]['series'][0]['values']]
            measurements = filter_measurements(measurements)

    if not measurements:
        print('Nothing to dump - empty database.')
        return

    print('Measurements:')
    print(measurements)
    print()

    # Get measurement fields.
    queries = []
    for m in measurements:
        if RETENTION:
            queries.append(f'SHOW FIELD KEYS FROM "{db}"."{RETENTION}"."{m}"')
        else:
            queries.append(f'SHOW FIELD KEYS FROM "{m}"')

    data = query_influxdb(params={'q': ';'.join(queries), 'db': db})
    msfields = {}
    for i in data['results']:
        # Empty measurement has no fields.
        if 'series' not in i:
            continue

        msfields[i['series'][0]['name']] = {x[0]: x[1] for x in i['series'][0]['values']}

    print('Measurement fields:')
    print(msfields)
    print()

    if not os.path.exists(DIR):
        os.makedirs(DIR)

    # Get series data.
    for m in measurements:
        if m not in msfields:
            # Empty measurement.
            print(f'Ignoring {m}... 0')
            continue

        print(f'Dumping {m}... ', end='')
        if GZIP:
            f = gzip.open(f'{DIR}/{measurement2filename(m)}.gz', 'wt')
        else:
            f = open(f'{DIR}/{measurement2filename(m)}', 'w')

        if RETENTION:
            query = f'SELECT * FROM "{db}"."{RETENTION}"."{m}" {where}'
        else:
            query = f'SELECT * FROM "{m}" {where}'

        line_count = 0
        for data in chunked_read(db, query):
            data = json.loads(data)
            rows = format_rows(m, msfields[m], data)
            f.writelines(rows)
            line_count += len(rows)
            if len(rows) == 0 and 'error' in data['results'][0]:
                # Possible error.
                print('ERROR', data['results'][0]['error'])
                sys.exit(-1)

        print(line_count)
        f.close()


def write_points(db, lines, chunk_delay, precision):
    """Write points to InfluxDB."""
    if chunk_delay:
        time.sleep(float(chunk_delay))

    data = ''.join(lines)
    params = {'db': db, 'precision': precision}
    if RETENTION:
        params['rp'] = RETENTION

    last_error = ''
    retries = 10
    while retries > 0:
        try:
            r = requests.post(URL+'/write', auth=AUTH, params=params, data=data.encode('utf-8'))
            if r.status_code == 204:
                return

            # InfluxDB is able to skip point beyond rp you write to
            if 'points beyond retention policy' in r.text:
                return

            last_error = f' {r.status_code} HTTP error, {r.text}'
        except Exception:
            last_error = sys.exc_info()[0]

        retries -= 1
        time.sleep(1)

    print(last_error)
    sys.exit(-1)


def restore(db, chunk_delay, measurement_delay, precision, force):
    """Restore from a backup."""
    if not os.path.exists(DIR):
        print(f'Backup dir "{DIR}" does not exist')
        sys.exit(-1)

    measurements = MEASUREMENTS
    if not measurements:
        if GZIP:
            files = [f[:-3] for f in os.listdir(DIR) if os.path.isfile(DIR+'/'+f) and f.endswith('.gz')]
        else:
            files = [f for f in os.listdir(DIR) if os.path.isfile(DIR+'/'+f) and not f.endswith('.gz')]

        files.sort()
        measurements = filter_measurements(files)
        measurements = [filename2measurement(mm) for mm in measurements]

    if not measurements:
        print('Nothing to restore. If backup is gzipped, use --gzip option.')
        sys.exit(-1)

    print('Files:')
    print(measurements)
    print()

    # Sanity check of timestamp precision.
    if GZIP:
        f = gzip.open(f'{DIR}/{measurement2filename(measurements[0])}.gz', 'rt')
    else:
        f = open(f'{DIR}/{measurement2filename(measurements[0])}', 'r')

    ts = len(f.readline().split()[-1])
    f.close()
    conditions = any([
        ts == 10 and precision != 's',
        ts == 13 and precision != 'ms',
        ts == 16 and precision != 'u',
        ts == 19 and precision != 'ns'
    ])
    if conditions:
        print('*** WARNING ***')
        print('Please check the precision. You may need to specify --restore-precision argument.')
        print(f'First line of the first file contains timestamp with length of {ts} while precision argument is set to "{precision}".')
        print('Generally, it is 10 for s, 13 for ms, 16 for u, 19 for ns.')
        print()

    if not force and input(f'> Confirm restore into "{db}" db? [yes/no] ') != 'yes':
        sys.exit()

    print()
    for m in measurements:
        if m != measurements[0] and measurement_delay:
            time.sleep(float(measurement_delay))

        print(f'Loading {measurement2filename(m)}... ', end='')
        lines = []
        line_count = 0
        if GZIP:
            f = gzip.open(f'{DIR}/{measurement2filename(m)}.gz', 'rt')
        else:
            f = open(f'{DIR}/{measurement2filename(m)}', 'r')

        for i in f:
            if len(lines) == WRITE_CHUNK_SIZE:
                write_points(db, lines, chunk_delay, precision)
                lines = []
                line_count += WRITE_CHUNK_SIZE

            lines.append(i)

        if lines:
            write_points(db, lines, chunk_delay, precision)

        print(line_count+len(lines))
        f.close()


def validate_date(date_str):
    """Validate date format."""
    try:
        return datetime.datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        pass

    try:
        datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError as err:
        print(err)
        sys.exit(-1)


def now():
    """Return formatted current time."""
    return datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='InfluxDB backup script')
    parser.add_argument('--url', required=True, help='InfluxDB URL including schema and port')
    parser.add_argument('--user', required=True, help='InfluxDB username. Password must be set as env var INFLUX_PW, otherwise will be asked.')
    parser.add_argument('--dir', required=True, help='directory name for backup or restore form')
    parser.add_argument('--measurements', help='comma-separated list of measurements to dump/restore')
    parser.add_argument('--ignore-measurements', help='comma-separated list of measurements to skip from dump/restore (ignored when using --measurements)')
    parser.add_argument('--from-measurement', help='dump/restore from this measurement and on (ignored when using --measurements)')
    parser.add_argument('--retention', help='retention to dump/restore')
    parser.add_argument('--gzip', action='store_true', help='dump/restore into/from gzipped files automatically')
    parser.add_argument('--dump', action='store_true', help='create a backup')
    parser.add_argument('--dump-db', help='database to dump')
    parser.add_argument('--dump-since', help='start date in the format YYYY-MM-DD (starting 00:00:00) or YYYY-MM-DDTHH:MM:SSZ')
    parser.add_argument('--dump-until', help='end date in the format YYYY-MM-DD (exclusive) or YYYY-MM-DDTHH:MM:SSZ')
    parser.add_argument('--restore', action='store_true', help='restore from a backup')
    parser.add_argument('--force', action='store_true', help='restore without prompt')
    parser.add_argument('--restore-db', help='database target of restore')
    parser.add_argument('--restore-precision', help='restore precision: ns,u,ms,s,m,h. Default: ns', default='ns')
    parser.add_argument('--restore-chunk-delay', help='restore delay in sec or subsec between chunks of %d points' % WRITE_CHUNK_SIZE)
    parser.add_argument('--restore-measurement-delay', help='restore delay in sec or subsec between measurements')
    args = parser.parse_args()

    if 'REQUESTS_CA_BUNDLE' not in os.environ:
        os.environ['REQUESTS_CA_BUNDLE'] = '/etc/ssl/certs/ca-certificates.crt'

    URL = args.url
    password = os.getenv('INFLUX_PW')
    if not password:
        password = getpass.getpass()

    AUTH = (args.user, password)
    DIR = args.dir
    GZIP = args.gzip
    RETENTION = args.retention
    FROM_MEASUREMENT = args.from_measurement
    MEASUREMENTS = args.measurements
    if MEASUREMENTS:
        MEASUREMENTS = MEASUREMENTS.split(',')

    IGNORE_MEASUREMENTS = args.ignore_measurements
    if IGNORE_MEASUREMENTS:
        IGNORE_MEASUREMENTS = IGNORE_MEASUREMENTS.split(',')

    # Enable unbuffered output.
    print = functools.partial(print, flush=True)

    if args.dump:
        if args.dump_db is None:
            print('--dump-db is required with --dump')
            parser.print_help()
            sys.exit(-1)

        WHERE = ''
        if args.dump_since and args.dump_until:
            validate_date(args.dump_since)
            validate_date(args.dump_until)
            WHERE = f"WHERE time >= '{args.dump_since}' AND time < '{args.dump_until}'"
        elif args.dump_since:
            validate_date(args.dump_since)
            WHERE = f"WHERE time >= '{args.dump_since}'"
        elif args.dump_until:
            validate_date(args.dump_until)
            WHERE = f"WHERE time < '{args.dump_until}'"

        print(f'>> {now()}')
        print(f'Starting backup of "{args.dump_db}" db to "{DIR}" dir {WHERE}\n')
        dump(args.dump_db, WHERE)
        print('Done.')
        print(f'<< {now()}')

    elif args.restore:
        if args.restore_db is None:
            print('--restore-db is required with --restore')
            parser.print_help()
            sys.exit(-1)

        if args.restore_precision not in ['ns', 'u', 'ms', 's', 'm', 'h']:
            print('--restore-precision should be one of ns,u,ms,s,m,h')
            sys.exit(-1)

        print(f'<< {now()}')
        print(f'Starting restore from "{DIR}" dir to "{args.restore_db}" db.\n')
        restore(args.restore_db, args.restore_chunk_delay, args.restore_measurement_delay, args.restore_precision, args.force)
        print('Done.')
        print(f'<< {now()}')

    else:
        parser.print_help()
