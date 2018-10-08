import numpy
import requests
import json
import psycopg2
import os
import random
import sys
import threading
import datetime
import traceback
import queue
import asyncio


keys = os.environ['API_KEY'].split('|')
cores = 1
timescale_queue = queue.Queue()
chunk_size = 50


def connect():
    return psycopg2.connect(user='admin', password='admin', host='192.168.1.63', port='5432', database='youtube')


def start_sql_service():
    conn = connect()

    insert_sql = f'INSERT INTO youtube.timeseries.subscriptions (chan_id, subs) VALUES (%s, %s)'
    while True:
        subs, ids, api_key = timescale_queue.get(block=True)
        assert(len(subs) == len(ids))
        cursor = conn.cursor()
        for data in zip(ids, subs):
            cursor.execute(insert_sql, data)
        conn.commit()
        cursor.close()
        print('Insert at', datetime.datetime.now(), len(subs), api_key)


def api_request(channels):
    key = random.choice(keys)
    chans = [list(x.keys())[0] for x in channels]
    serial_id = {}
    for c in channels:
        serial_id[list(c.keys())[0]] = list(c.values())[0]

    url = 'https://www.googleapis.com/youtube/v3/channels'
    params = {
        'part': 'snippet,statistics',
        'id': ','.join(chans),
        'key': key
    }

    req = requests.get(url, params=params)
    json_body = json.loads(req.text)
    if 'items' not in json_body:
        return None

    ids = []
    for item in json_body['items']:
        ids.append(serial_id[item['id']])

    return json_body, ids, key


def extract_stats(json_body):
    stats_result = json_body['items']

    stats_body = []
    for s in stats_result:
        stats_tmp = s['statistics']
        stats_body.append(int(stats_tmp['subscriberCount']))

    return stats_body


def query_channels():
    conn = connect()
    sql = f'SELECT chan_serial, subs, id FROM youtube.entities.chans ORDER BY subs DESC'
    cursor = conn.cursor()
    cursor.execute(sql)
    records = [x for x in cursor.fetchall()]

    cursor.close()
    conn.close()

    return records


def weighted_distro(chans):
    channels = [{c[0]: c[-1]} for c in chans]
    subs = [c[1] for c in chans]
    total_sum = sum(subs)

    weights = [s / total_sum for s in subs]
    return channels, weights


def get_sample(distro, n):
    chans = distro[0]
    weights = distro[1]
    return [numpy.random.choice(chans, p=weights) for x in range(n)]


async def parse_request(distro):
    try:
        sample = get_sample(distro, chunk_size)
        result = api_request(sample)
        if result is None:
            return

        json_body, ids, key = result

        stats = extract_stats(json_body)
        timescale_queue.put((stats, ids, key))
    except Exception as e:
        print(e, file=sys.stderr)
        traceback.print_exc()


def async_wrapper(distro):
    asyncio.run(parse_request(distro))


def main():
    threading.Thread(target=start_sql_service, daemon=True).start()

    chans_weight = query_channels()
    distro = weighted_distro(chans_weight)

    def func():
        while True:
            async_wrapper(distro)

    for i in range(cores):
        threading.Thread(target=func).start()


if __name__ == '__main__':
    main()
