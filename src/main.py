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
influx_queue = queue.Queue()
chunk_size = 50


def connect(db):
    return psycopg2.connect(user='root', password='', host='127.0.0.1', port='5432', database=db)


def start_sql_service():
    conn = connect('timeseries')

    insert_sql = f'INSERT INTO subscriptions (channel_serial, sub) VALUES (%s, %s)'
    while True:
        subs, api_key = influx_queue.get(block=True)
        cursor = conn.cursor()
        for data in subs:
            cursor.execute(insert_sql, data)
        conn.commit()
        cursor.close()
        print('Insert at', datetime.datetime.now(), len(subs), api_key)


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
    return json_body, key


def extract_stats(json_body):
    stats_result = json_body['items']

    stats_body = []
    for s in stats_result:
        stats_tmp = s['statistics']
        stats_body.append(int(stats_tmp['subscriberCount']))

    return stats_body


def query_channels():
    conn = connect('youtube')
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
    return [numpy.random.choice(chans, p=weights) for x in range(n)]


async def parse_request(distro):
    try:
        sample = get_sample(distro[0], distro[1], chunk_size)
        json_body, key = api_request(sample)

        stats = extract_stats(json_body)

        influx_queue.put((list(zip(sample, stats)), key))
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
