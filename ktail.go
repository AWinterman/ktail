package main

import (
	"os"
	"fmt"
	"time"
	"runtime"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/codegangsta/cli"
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
		params := &kinesis.GetRecordsInput{
			ShardIterator: aws.String(shardIterator),
			Limit:         aws.Int64(1000),
		}
		resp, err := svc.GetRecords(params)
		if err != nil {
			panic(err)
		}

		for _, record := range resp.Records {
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

func printRecords(messageChannel chan kinesis.Record, fields []string) {
	for {
		select {
		case record := <-messageChannel:
			var event interface{}
			json.Unmarshal(record.Data, &event)

			eventMap := event.(map[string]interface{})

			if len(fields) > 0 {
				for _, field := range fields {
					fmt.Print(eventMap[field])
					fmt.Print("  ")
				}
				fmt.Println()
			} else {
				fmt.Println(event)
			}
		}
	}
}


func readStream(streamName string, fields []string) {
	streamShards := getStreamShards(streamName)

	messageChannel := make(chan kinesis.Record, 100)
	var waitGroup sync.WaitGroup

	for _, streamShard := range streamShards {

		waitGroup.Add(1)
		go func(shard kinesis.Shard) {
			defer waitGroup.Done()
			readShard(streamName, shard, messageChannel)
		}(*streamShard)
	}

	printRecords(messageChannel, fields)
	waitGroup.Wait()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	app := cli.NewApp()
	app.EnableBashCompletion = true
	app.Name = "ktail"
	app.Usage = "read json messages from AWS Kinesis streams"
	app.Flags = []cli.Flag{
		cli.StringSliceFlag{
			Name:            "field",
			Usage:            "define field to print instead of the complete message",
		},
	}
	app.Action = func(c *cli.Context) {
		if len(c.Args()) == 0 {
			for _, streamName := range getStreamNames() {
				fmt.Println(*streamName)
			}
		} else if len(c.Args()) > 0 {
			streamName := c.Args()[0]
			filterFields := c.StringSlice("field")

			readStream(streamName, filterFields)
		}
	}

	app.Run(os.Args)
}

