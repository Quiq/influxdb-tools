#!/usr/bin/env python

"""Create continuous queries for InfluxDB according to the list of metrics."""

import optparse
import yaml

import influxdb

# Labels for aggregation by default
DEFAULT_GROUP_BY = 'cluster, instance'
# Downsampling intervals/retention policy names
INTERVALS = ['5m', '1h']

METRICS = yaml.load("""
node_cpu: {type: counter, group_by: 'mode, cpu'}
node_disk_bytes_read: {type: counter, group_by: device}
node_disk_bytes_written: {type: counter, group_by: device}
node_disk_reads_completed: {type: counter, group_by: device}
node_disk_writes_completed: {type: counter, group_by: device}
node_filesystem_avail: {type: gauge, group_by: 'mountpoint, fstype, device'}
node_filesystem_size: {type: gauge, group_by: 'mountpoint, fstype, device'}
node_load1: {type: gauge}
node_memory_Buffers: {type: gauge}
node_memory_Cached: {type: gauge}
node_memory_MemAvailable: {type: gauge}
node_memory_MemFree: {type: gauge}
node_memory_MemTotal: {type: gauge}
node_network_receive_bytes: {type: counter, group_by: device}
node_network_transmit_bytes: {type: counter, group_by: device}
""")


def main():
    """Main."""
    parser = optparse.OptionParser()
    parser.add_option('--host', default='localhost', help='InfluxDB host')
    parser.add_option('--port', default=8086, help='InfluxDB port')
    parser.add_option('--user', default='admin', help='InfluxDB username')
    parser.add_option('--pass', default='admin', help='InfluxDB password')
    parser.add_option('--prom-db', default='prometheus', help='InfluxDB db with raw prometheus data')
    parser.add_option('--trend-db', default='trending', help='InfluxDB db for trending data')
    parser.add_option('--drop-trend-db', action='store_true', default=False, help='Drop trending db')
    parser.add_option('--exit-on-cq', action='store_true', default=False, help='Exit when any continuous queries exist')
    options, _ = parser.parse_args()

    client = influxdb.InfluxDBClient(options.host, options.port, options.username, options.password, options.prom_db)

    # Drop all existing CQ from prometheus db
    queries = []
    result = client.query('SHOW CONTINUOUS QUERIES;')
    for i in result.raw['series']:
        if i['name'] == options.prom_db and 'values' in i:
            queries = [i[0] for i in i['values']]

    # Exit when --check-cq is set and CQ already exist
    if options.exit_on_cq and queries:
        print '[%s] %s continuous queries exist.' % (options.prom_db, len(queries))
        return

    count = 0
    for name in queries:
        client.query('DROP CONTINUOUS QUERY %s ON %s;' % (name, options.prom_db))
        count += 1

    print '[%s] Deleted %s continuous queries.' % (options.prom_db, count)

    # Recreate trending db
    if options.drop_trend_db:
        client.drop_database(options.trend_db)
        print '[%s] Database dropped.' % (options.trend_db,)

    dbs = [x['name'] for x in client.get_list_database()]
    if options.trend_db not in dbs:
        client.create_database(options.trend_db)
        print '[%s] Database created.' % (options.trend_db,)

    # Create new CQ
    count = 0
    retentions = [x['name'] for x in client.get_list_retention_policies(database=options.trend_db)]
    for interval in INTERVALS:
        # Create retention
        if interval not in retentions:
            client.create_retention_policy('"%s"' % (interval,), 'INF', '1', options.trend_db)
            print '[%s] Retention policy "%s" created.' % (options.trend_db, interval)

        for metric, data in METRICS.iteritems():
            params = {
                'metric': metric,
                'interval': interval,
                'prom_db': options.prom_db,
                'trend_db': options.trend_db,
                'select': '',
                'group_by': DEFAULT_GROUP_BY
            }
            # Averaging for gauges, maxing for counters
            if data['type'] == 'gauge':
                params['select'] = 'MEAN(value)'
            elif data['type'] == 'counter':
                params['select'] = 'MAX(value)'

            if 'group_by' in data:
                params['group_by'] = '%s, %s' % (params['group_by'], data['group_by'])

            query = """CREATE CONTINUOUS QUERY %(metric)s_%(interval)s ON %(prom_db)s
                BEGIN
                    SELECT %(select)s INTO %(trend_db)s."%(interval)s".%(metric)s
                    FROM %(metric)s GROUP BY time(%(interval)s), %(group_by)s
                END;
            """ % params
            client.query(query)
            count += 1

    print '[%s] Added %s continuous queries.' % (options.prom_db, count)


if __name__ == '__main__':
    main()
