import json
import logging
import io
import csv
import gzip
from pprint import pprint

import boto3
import requests
from smart_open import open

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):

    file_url = event['file_url']
    s3_bucket = event['s3_bucket']
    key = event['key']

    chunk_size = 1024*1024*8

    logger.info('fetching {} to {}'.format(file_url, key))

    s3 = boto3.client('s3')
    obj_list = s3.list_objects_v2(Bucket=s3_bucket, Prefix=key)

    if obj_list.get('KeyCount', 0) > 0:
        logger.warn('trying to download {} but it already exists -- skipping'.format(key))
        return({
            'message': 'key {} already exists - skipping'.format(key)
        })

    with open('s3://{}/{}.csv.gz'.format(s3_bucket, key), 'wb', ignore_ext=True) as fout:
        gzipout = gzip.GzipFile(fileobj=fout, mode='wb')
        csvout = csv.writer(io.TextIOWrapper(gzipout, write_through=True)) 
        with requests.get(file_url, stream=True) as r:
            r.raise_for_status()
            gzipin = gzip.GzipFile(fileobj=r.raw, mode='rb')
            buffer = io.BytesIO()
            for chunk in iter(partial(gzipin.read, chunk_size), b''):
                buffer.write(chunk)
                buffer.seek(0)
                with io.TextIOWrapper(buffer) as text_buffer:
                    with io.StringIO() as csvin:
                        for line in text_buffer:
                            if line.endswith('\n'):
                                csvin.write(line)
                        csvin.seek(0)
                        for row in csv.reader(csvin, delimiter='\t'):
                            csvout.writerow(row)
                buffer = io.BytesIO()
                if not line.endswith('\n'):
                    buffer.write(line.encode())

            gzipout.close()

    return {
        'statusCode': 200,
    }
