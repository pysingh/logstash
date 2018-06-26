# -*- coding: utf-8 -*-

import urllib.request
import json
import time

#List of instaces (IP address)
HOST_LIST = []

#flag for point if intanse is using loadstash version 5 or 6
IS_VERSION_FIVE = False

def get_response(url):
    '''
        Function for getting the response of requested URL
    '''
    req = urllib.request.Request(url)
    ##parsing response
    r = urllib.request.urlopen(req).read()
    return json.loads(r.decode('utf-8'))

def set_logstash_version(version):
    if '5.6'in version:
        IS_VERSION_FIVE = True
    else:
        IS_VERSION_FIVE = False

def format_stats(value, metric_name, host):
    metric_type = 'gauge'
    unix_epoch_timestamp = int(time.time())
    tag_list = ['#service-name:card-fb0614', '#host-address:{0}'.format(host)]
    print('MONITORING|{0}|{1}|{2}|{3}|{4}'.format(
        unix_epoch_timestamp, value, metric_type, metric_name, ', '.join(tag_list)
    ))

def format_url(host, end_point):
    return 'http://{0}:9600{1}'.format(host, end_point)

def  get_jvm_stats(host, end_point):
    url = format_url(host, end_point)
    response = get_response(url)
    #Setting logstash version
    set_logstash_version(response['version'])
    #printing data for jvm count
    format_stats(response['jvm']['threads']['count'], 'logstash.jvm.threads.count', host)

    #printing data for peak count
    format_stats(response['jvm']['threads']['peak_count'], 'logstash.jvm.threads.peak_count', host)

    #Printing data for memory
    format_stats(response['jvm']['mem']['heap_used_in_bytes'], 'logtash.jvm.mem.heap_used_in_bytes', host)
    format_stats(response['jvm']['mem']['heap_used_percent'], 'logtash.jvm.mem.heap_used_percent', host)
    format_stats(response['jvm']['mem']['heap_committed_in_bytes'], 'logtash.jvm.mem.heap_committed_in_bytes', host)
    format_stats(response['jvm']['mem']['heap_max_in_bytes'], 'logtash.jvm.mem.heap_max_in_bytes', host)
    format_stats(response['jvm']['mem']['non_heap_used_in_bytes'], 'logtash.jvm.mem.non_heap_used_in_bytes', host)

def get_process_stats(host, end_point):
    url = format_url(host, end_point)
    response = get_response(url)

    format_stats(response['process']['mem']['total_virtual_in_bytes'], 'logstash.process.mem.total_virtual_in_bytes', host)
    format_stats(response['process']['cpu']['total_in_millis'], 'logstash.process.cpu.total_in_millis', host)
    format_stats(response['process']['cpu']['percent'], 'logstash.process.cpu.percent', host)
    format_stats(response['process']['cpu']['load_average']['1m'], 'logstash.process.cpu.load_average.1m', host)
    format_stats(response['process']['cpu']['load_average']['5m'], 'logstash.process.cpu.load_average.5m', host)
    format_stats(response['process']['cpu']['load_average']['15m'], 'logstash.process.cpu.load_average.15m', host)

def get_event_stats(host, end_point):
    url = format_url(host, end_point)
    response = get_response(url)

    format_stats(response['events']['duration_in_millis'], 'logstash.events.duration_in_millis', host)
    format_stats(response['events']['in'], 'logstash.events.in', host)
    format_stats(response['events']['out'], 'logstash.events.out', host)
    format_stats(response['events']['filtered'], 'logstash.events.filtered', host)
    format_stats(response['events']['queue_push_duration_in_millis'], 'logstash.events.queue_push_duration_in_millis', host)

def get_pipeline_stats(host, end_point):
    url = format_url(host, end_point)
    response = get_response(url)

    format_stats(response['pipeline']['events']['duration_in_millis'], 'logstash.events.duration_in_millis', host)
    format_stats(response['pipeline']['events']['in'], 'logstash.events.in', host)
    format_stats(response['pipeline']['events']['out'], 'logstash.events.out', host)
    format_stats(response['pipeline']['events']['filtered'], 'logstash.events.filtered', host)
    format_stats(response['pipeline']['events']['queue_push_duration_in_millis'], 'logstash.events.queue_push_duration_in_millis', host)


for host in HOST_LIST:
    '''
        Interating over the IP addresses
    '''
    try:
        get_jvm_stats(host, '/_node/stats/jvm?pretty')
    except Exception as e:
        pass
    try:
        get_process_stats(host, '/_node/stats/process?pretty')
    except Exception as e:
        pass
    if IS_VERSION_FIVE:
        try:
            get_process_stats(host, '/_node/stats/pipeline?pretty')
        except Exception as e:
            pass
    else:
        try:
            get_process_stats(host, '/_node/stats/events?pretty')
        except Exception as e:
            pass