import numpy
import requests
import json
import psycopg2
import math
import os
import random
import sys
import threading
import datetime
import traceback
import queue
import influxdb
from multiprocessing.dummy import Pool


keys = os.environ['API_KEY'].split('|')
cores = 4
pool = Pool(cores)
influx_queue = queue.Queue()
chunk_size = 50


def start_influx_service():
    client = influxdb.InfluxDBClient('localhost', 8086, 'admin', 'admin', 'youtube')

    while True:
        json_single = influx_queue.get(block=True)
        client.write_points(json_single)
        print('Insert at', datetime.datetime.now(), len(json_single))


def influx_json_format(name, fields):
    return {
        'measurement': 'Channels',
        'tags': name,
        'time': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'fields': fields
    }


def api_request(chans):
    key = random.choice(keys)

    url = 'https://www.googleapis.com/youtube/v3/channels'
    params = {
        'part': 'snippet,statistics',
        'id': ','.join(chans),
        'key': key
    }

    req = requests.get(url, params=params)
    json_body = json.loads(req.text)
    return json_body


def extract_stats(json_body):
    stats_result = json_body['items']

    stats_body = []
    for s in stats_result:
        stats_tmp = s['statistics']
        stats_body.append({
            'viewCount': int(stats_tmp['viewCount']),
            'subscriberCount': int(stats_tmp['subscriberCount']),
            'videoCount': int(stats_tmp['videoCount'])
        })

    return stats_body


def extract_title(json_body):
    items = json_body['items']

    titles = []
    for i in items:
        titles.append(i['snippet']['title'])

    return titles


def query_channels():
    conn = psycopg2.connect(user='root', password='', host='127.0.0.1', port='5432', database='youtube')
    sql = f'SELECT chan_serial, subs FROM youtube.channels.chans ORDER BY subs DESC'
    cursor = conn.cursor()
    cursor.execute(sql)
    records = [x for x in cursor.fetchall()]

    cursor.close()
    conn.close()

    return records


def weighted_distro(chans):
    channels = [c[0] for c in chans]
    subs = [c[1] for c in chans]
    total_sum = sum(subs)

    weights = [s / total_sum for s in subs]
    return channels, weights


def get_sample(chans, weights, n):
    return [numpy.random.choice(chans, p=weights) for x in chans]


def parse_request(distro):
    try:
        sample = get_sample(distro[0], distro[1], chunk_size)
        json_body = api_request(sample)

        titles = extract_title(json_body)
        stats = extract_stats(json_body)

        body = [influx_json_format({'name': titles[i]}, stats[i]) for i in range(len(stats))]
        influx_queue.put(body)
    except Exception as e:
        print(e, file=sys.stderr)
        traceback.print_exc()


def main():
    threading.Thread(target=start_influx_service, daemon=True).start()

    chans_weight = query_channels()
    distro = weighted_distro(chans_weight)

    iter = [distro] * cores
    while True:
        pool.map(parse_request, iter)



if __name__ == '__main__':
    main()
