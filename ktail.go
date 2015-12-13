package main

import (
	"fmt"
	"time"
	"runtime"
	"reflect"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"encoding/json"
	"sync"
)

func Kinesis() *kinesis.Kinesis {
	return kinesis.New(session.New(), &aws.Config{Region: aws.String("eu-west-1")})
}

func getStreamNames() []*string {
	svc := Kinesis();
	hasMoreItems := true;
	var streamNames []*string;

	for hasMoreItems {
		resp, err := svc.ListStreams(nil)
		if err != nil {
			panic(err)
		}
		hasMoreItems = *resp.HasMoreStreams
		fmt.Println(reflect.TypeOf(resp.StreamNames))
		//fmt.Println(*resp.StreamNames[0])
		streamNames = append(streamNames, resp.StreamNames...)
	}
	return streamNames
}

func getStreamShards(StreamName string) []*kinesis.Shard {
	svc := Kinesis();
	var hasMoreShards bool = true;
	var exclusiveStartShardId string;
	var streamShards []*kinesis.Shard;

	for hasMoreShards {
		params := &kinesis.DescribeStreamInput{
			StreamName:            aws.String(StreamName),
			Limit: aws.Int64(10),
		}

		if exclusiveStartShardId != "" {
			params.ExclusiveStartShardId = aws.String(exclusiveStartShardId)
		}

		resp, err := svc.DescribeStream(params)
		if err != nil {
			panic(err)
		}

		shardsPage := resp.StreamDescription.Shards
		streamShards = append(streamShards, shardsPage...)

		exclusiveStartShardId = *shardsPage[len(shardsPage)-1].ShardId
		hasMoreShards = *resp.StreamDescription.HasMoreShards
	}
	return streamShards
}

func getShardIterator(streamName string, shard kinesis.Shard) string {
	svc := Kinesis();

	params := &kinesis.GetShardIteratorInput{
		ShardId: aws.String(*shard.ShardId),
		ShardIteratorType: aws.String("TRIM_HORIZON"),
		StreamName: aws.String(streamName),
	}

	resp, err := svc.GetShardIterator(params)
	if err != nil {
		panic(err)
	}

	return *resp.ShardIterator
}

func readShard(streamName string, streamShard kinesis.Shard, messageChannel chan kinesis.Record) {
	svc := Kinesis();
	shardIterator := getShardIterator(streamName, streamShard)

	for shardIterator != "" {
		//fmt.Println("Reading shard:", *streamShard.ShardId)

		params := &kinesis.GetRecordsInput{
			ShardIterator: aws.String(shardIterator),
			Limit:         aws.Int64(10),
		}
		resp, err := svc.GetRecords(params)
		if err != nil {
			panic(err)
		}

		for _, record  := range resp.Records {
			messageChannel <- *record
		}

		if resp.NextShardIterator != nil {
			shardIterator = *resp.NextShardIterator
		} else {
			shardIterator = ""
		}

		time.Sleep(time.Second)
	}

}

func printMessages(messageChannel chan kinesis.Record) {
	for {
		select {
		case record := <-messageChannel:
			var event interface{};
			json.Unmarshal(record.Data, &event)
			fmt.Println(event)
			//for k,v := range event.(map[string]interface {}) {
			//	fmt.Println(k," : ",v)
			//}
			//fmt.Println(event)
		}
	}
}


func readStream(streamName string) {
	streamShards := getStreamShards(streamName)

	messageChannel := make(chan kinesis.Record, 10)
	var waitGroup sync.WaitGroup

	for _, streamShard := range streamShards {

		waitGroup.Add(1)

		go func(shard kinesis.Shard) {
			defer waitGroup.Done()
			readShard(streamName, shard, messageChannel)
		}(*streamShard)
	}

	printMessages(messageChannel)
	waitGroup.Wait()
}

func main() {
	fmt.Println("Will use", runtime.NumCPU(), "cpu threads")
	runtime.GOMAXPROCS(runtime.NumCPU())

	readStream("logging-demo-log-stream-kinesisStream")
}
