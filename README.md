# InfluxDB Tools

## influx-backup.py

InfluxDB backup/restore script using HTTP API and line-protocol format.

* Use InfluxDB HTTP API
* Backup raw data into text files in line-protocol format
* Restore from a backup
* Chunked read/write
* Separate file for each measurement
* Backup/restore individual measurements
* Backup/restore specific retention
* Incremental backups using "since", "until" date/time arguments
* Delayed restore
* Gzip support for backup/restore process

Requires Python `3.5+` and `requests` module. Tested on InfluxDB `1.3.6`.

It is recommended to do a delayed restore using `--restore-chunk-delay`, `--restore-measurement-delay`
so your InfluxDB instance does not run out of memory or IO pretty fast.

### Usage
```
usage: influx-backup.py [-h] --url URL --user USER --dir DIR
                        [--measurements MEASUREMENTS]
                        [--from-measurement FROM_MEASUREMENT]
                        [--retention RETENTION] [--gzip] [--dump]
                        [--dump-db DUMP_DB] [--dump-since DUMP_SINCE]
                        [--dump-until DUMP_UNTIL] [--restore]
                        [--restore-db RESTORE_DB]
                        [--restore-chunk-delay RESTORE_CHUNK_DELAY]
                        [--restore-measurement-delay RESTORE_MEASUREMENT_DELAY]

InfluxDB backup script

optional arguments:
  -h, --help            show this help message and exit
  --url URL             InfluxDB URL including schema and port
  --user USER           InfluxDB username. Password must be set as env var
                        INFLUX_PW, otherwise will be asked.
  --dir DIR             directory name for backup or restore form
  --measurements MEASUREMENTS
                        comma-separated list of measurements to dump/restore
  --from-measurement FROM_MEASUREMENT
                        dump/restore from this measurement and on (ignored
                        when using --measurements)
  --retention RETENTION
                        retention to dump/restore
  --gzip                dump/restore into/from gzipped files automatically
  --dump                create a backup
  --dump-db DUMP_DB     database to dump
  --dump-since DUMP_SINCE
                        start date in the format YYYY-MM-DD (starting
                        00:00:00) or YYYY-MM-DDTHH:MM:SSZ
  --dump-until DUMP_UNTIL
                        end date in the format YYYY-MM-DD (exclusive)
                        or YYYY-MM-DDTHH:MM:SSZ
  --restore             restore from a backup
  --restore-db RESTORE_DB
                        database target of restore
  --restore-chunk-delay RESTORE_CHUNK_DELAY
                        restore delay in sec or subsec between chunks of 5000
                        points
  --restore-measurement-delay RESTORE_MEASUREMENT_DELAY
                        restore delay in sec or subsec between measurements
```

### Examples

Dump `stats` db:
```
./influx-backup.py --url https://influxdb.localhost:8086 --user admin --dump --dump-db stats --dir stats
```
Dump `heartbeat` measurement from `stats` db with data until 2017-09-01:
```
./influx-backup.py --url https://influxdb.localhost:8086 --user admin --dump --dump-db stats --dir stats \
    --dump-until 2017-09-01 --measurements heartbeat
```
NOTE: If you get `ChunkedEncodingError` on dump, try to limit the data set using "since", "until" arguments.

Restore from `stats` dir into `stats_new` db:
```
./influx-backup.py --url https://influxdb.localhost:8086 --user admin --restore --restore-db stats_new \
    --dir stats
```
Restore only `heartbeat` measurement from `stats` dir into `stats_new` db:
```
./influx-backup.py --url https://influxdb.localhost:8086 --user admin --restore --restore-db stats_new \
    --dir stats --measurements heartbeat
```

## influx-cq.py

Create continuous queries for InfluxDB according to the list of metrics.

[Work in progess]

## Usage
```
usage: influx-cq.py [-h] [--host HOST] [--port PORT] [--user USER]
                    [--pass PASS] [--prom-db PROM_DB] [--trend-db TREND_DB]
                    [--drop-trend-db] [--exit-on-cq]

Script for creating CQ queries on InfluxDB

optional arguments:
  -h, --help           show this help message and exit
  --host HOST          InfluxDB host
  --port PORT          InfluxDB port
  --user USER          InfluxDB username
  --pass PASS          InfluxDB password
  --prom-db PROM_DB    InfluxDB db with raw prometheus data
  --trend-db TREND_DB  InfluxDB db for trending data
  --drop-trend-db      Drop trending db
  --exit-on-cq         Exit when any continuous queries exist
```
