from boto3 import client
import threading
import time
import json

__version__ = '${version}'


class KinesisClient(object):
    def __init__(self, region):
        self.conn = client('kinesis', region_name=region)

    def list_streams(self):
        response = self.conn.list_streams()
        stream_names = response['StreamNames']

        while response['HasMoreStreams']:
            response = self.conn.list_streams(ExclusiveStartStreamName=stream_names[-1])
            stream_names.extend(response['StreamNames'])

        return stream_names

    def get_stream_shard_iterator(self, stream_name, shart_id):
        response = self.conn.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shart_id,
            ShardIteratorType='LATEST',
        )
        return response['ShardIterator']

    def get_stream_shards(self, stream_name):
        response = self.conn.describe_stream(
            StreamName=stream_name
        )

        return response['StreamDescription']['Shards']

    def get_json_events_from_stream(self, stream_name, fields):
        threads = []

        shards = self.get_stream_shards(stream_name)
        print "Starting {0} threads, one for each shard of the stream".format(len(shards))
        for shard in shards:
            shard_id = shard['ShardId']
            worker_name = "shard_worker_{0}".format(shard_id)

            worker = KinesisStreamShardReader(self.conn, stream_name, shard_id, fields=fields, name=worker_name)
            worker.daemon = True
            threads.append(worker)
            worker.start()

        for t in threads:
            t.join()


class KinesisStreamShardReader(threading.Thread):
    def __init__(self, conn, stream_name, shard_id, fields=None, name=None, group=None, echo=False, args=(), kwargs={}):
        self.conn = conn
        self.stream_name = stream_name
        self.shard_id = shard_id
        self.fields = fields

        super(KinesisStreamShardReader, self).__init__(name=name, group=group, args=args, kwargs=kwargs)

    def get_stream_shard_iterator(self):
        response = self.conn.get_shard_iterator(
            StreamName=self.stream_name,
            ShardId=self.shard_id,
            ShardIteratorType='LATEST',
        )

        return response['ShardIterator']

    def print_event(self, event):
        if self.fields:
            print(' '.join([str(value) for key, value in event.items() if key in self.fields]))
        else:
            print(' '.join(["{0}={1}".format(str(key), str(value)) for key, value in event.items()]))

    def run(self):
        shard_iterator = self.get_stream_shard_iterator()

        while shard_iterator:
            response = self.conn.get_records(ShardIterator=shard_iterator)
            shard_iterator = response['NextShardIterator']
            records = response['Records']
            for record in records:
                try:
                    event = json.loads(record['Data'])
                    self.print_event(event)
                except Exception as e:
                    print "Could not deserialize kinesis record: {0}".format(e)

            time.sleep(1)
