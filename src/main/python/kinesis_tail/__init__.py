from boto3 import client
import asyncio
import time
import json

__version__ = '${version}'


class KinesisClient(object):
    def __init__(self, region, debug=False):
        self.debug = debug
        self.region = region
        self.conn = client('kinesis', region_name=region)

    def list_streams(self):
        response = self.conn.list_streams()
        stream_names = response['StreamNames']

        while response['HasMoreStreams']:
            response = self.conn.list_streams(ExclusiveStartStreamName=stream_names[-1])
            stream_names.extend(response['StreamNames'])

        return stream_names

    def _get_stream_shards(self, stream_name):
        response = self.conn.describe_stream(
            StreamName=stream_name
        )

        return response['StreamDescription']['Shards']

    @asyncio.coroutine
    def read_shard(self, stream_name, shard_id, fields):
        KinesisStreamShardReader(self.region, stream_name, shard_id, debug=self.debug,
                                 fields=fields).read_shard()

    def get_json_events_from_stream(self, stream_name, fields):
        loop = asyncio.get_event_loop()
        tasks = []

        shards = self._get_stream_shards(stream_name)

        for shard in shards:
            shard_id = shard['ShardId']
            tasks.append(self.read_shard(stream_name, shard_id, fields))

        loop.run_until_complete(asyncio.wait(tasks))
        loop.close()


class KinesisStreamShardReader(object):
    def __init__(self, region, stream_name, shard_id, debug=False, fields=None):
        self.debug = debug
        self.conn = client('kinesis', region_name=region)
        self.stream_name = stream_name
        self.shard_id = shard_id
        self.fields = fields

    def _get_stream_shard_iterator(self):
        response = self.conn.get_shard_iterator(
            StreamName=self.stream_name,
            ShardId=self.shard_id,
            ShardIteratorType='LATEST',
        )

        return response['ShardIterator']

    def _print_event(self, event):
        if self.fields:
            print(' '.join([str(event[field]) for field in self.fields]))
        else:
            print(' '.join(["{0}={1}".format(str(key), str(value)) for key, value in event.items()]))

    def read_shard(self):
        shard_iterator = self._get_stream_shard_iterator()

        while shard_iterator:
            response = self.conn.get_records(ShardIterator=shard_iterator)
            shard_iterator = response['NextShardIterator']
            records = response['Records']

            if self.debug:
                print("Received {0} events from shard {1}".format(len(records), self.shard_id))

            for record in records:
                try:
                    if self.debug:
                        print("Raw Kinesis record: {0}".format(record))
                    event = json.loads(record['Data'].decode('utf-8'))
                    self._print_event(event)
                except Exception as e:
                    print("Could not deserialize kinesis record: {0}".format(e))
                    if self.debug:
                        print("Raw event: {0}".format(record['Data']))

            time.sleep(1)
