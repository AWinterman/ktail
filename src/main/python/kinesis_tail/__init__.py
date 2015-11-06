from boto3 import client
from multiprocessing import Process
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

    def get_json_events_from_stream(self, stream_name, fields):
        threads = []

        shards = self._get_stream_shards(stream_name)
        if self.debug:
            print "Spawning {0} processes, one for each shard of stream {1}".format(len(shards), stream_name)

        for shard in shards:
            shard_id = shard['ShardId']
            worker_name = "shard_reader_{0}".format(shard_id)

            worker = KinesisStreamShardReader(self.region, stream_name, shard_id, debug=self.debug,
                                              fields=fields, name=worker_name)
            worker.daemon = False
            threads.append(worker)
            worker.start()

        for t in threads:
            t.join()


class KinesisStreamShardReader(Process):
    def __init__(self, region, stream_name, shard_id, debug=False, fields=None, name=None, group=None, echo=False,
                 args=(), kwargs={}):
        self.debug = debug
        self.conn = client('kinesis', region_name=region)
        self.stream_name = stream_name
        self.shard_id = shard_id
        self.fields = fields

        super(KinesisStreamShardReader, self).__init__(name=name, group=group, args=args, kwargs=kwargs)

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

    def _read_shard(self):
        shard_iterator = self._get_stream_shard_iterator()

        while shard_iterator:
            response = self.conn.get_records(ShardIterator=shard_iterator)
            shard_iterator = response['NextShardIterator']
            records = response['Records']

            if self.debug:
                print "Received {0} events from shard {1}".format(len(records), self.shard_id)

            for record in records:
                try:
                    event = json.loads(record['Data'])
                    self._print_event(event)
                except Exception as e:
                    print "Could not deserialize kinesis record: {0}".format(e)
                    if self.debug:
                        print("Raw event: {0}".format(record['Data']))

            time.sleep(1)

    def run(self):
        try:
            self._read_shard()
        except KeyboardInterrupt:
            pass
        except Exception as e:
            print "Error reading shard {0}: {1}".format(self.shard_id, e)
            if self.debug:
                import traceback

                traceback.print_exc()
