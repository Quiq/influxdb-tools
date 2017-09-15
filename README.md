# InfluxDB Tools

## influx-backup.py

InfluxDB backup/restore script using HTTP API and line-protocol format.

* Use InfluxDB HTTP API
* Backup raw data into text files in line-protocol format
* Restore from a backup
* Chunked read/write
* Separate file for each measurement
* Backup/restore individual measurements
* Incremental backups using "since", "until" arguments

Tested on InfluxDB version `1.3.5`.

Requires python `2.7` and `requests` module.

### Usage
```
usage: influx-backup.py [-h] --url URL --user USER --dir DIR
                        [--measurements MEASUREMENTS]
                        [--from-measurement FROM_MEASUREMENT] [--dump]
                        [--dump-db DUMP_DB] [--dump-since DUMP_SINCE]
                        [--dump-until DUMP_UNTIL] [--restore]
                        [--restore-db RESTORE_DB] [--restore-rp RESTORE_RP]
                        [--restore-delay RESTORE_DELAY]

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
  --dump                create a backup
  --dump-db DUMP_DB     database to dump
  --dump-since DUMP_SINCE
                        start date in the format YYYY-MM-DD
  --dump-until DUMP_UNTIL
                        end date in the format YYYY-MM-DD
  --restore             restore from a backup
  --restore-db RESTORE_DB
                        database target of restore
  --restore-rp RESTORE_RP
                        retention to restore to
  --restore-delay RESTORE_DELAY
                        restore delay in sec or subsec between chunks of 5000
                        points
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
