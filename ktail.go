package main

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/codegangsta/cli"
	"runtime"
	"sync"
	"time"
)

func Kinesis(Region string) *kinesis.Kinesis {
	return kinesis.New(session.New(), aws.NewConfig().WithRegion(Region))
}

func getStreamNames(Region string) []*string {
	svc := Kinesis(Region)
	hasMoreItems := true
	var streamNames []*string

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

func getStreamShards(StreamName string, Region string) []*kinesis.Shard {
	svc := Kinesis(Region)
	var hasMoreShards bool = true
	var exclusiveStartShardId string
	var streamShards []*kinesis.Shard

	for hasMoreShards {
		params := &kinesis.DescribeStreamInput{
			StreamName: aws.String(StreamName),
			Limit:      aws.Int64(10),
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

func getShardIterator(streamName string, shard kinesis.Shard, region string) string {
	svc := Kinesis(region)

	params := &kinesis.GetShardIteratorInput{
		ShardId:           aws.String(*shard.ShardId),
		ShardIteratorType: aws.String("LATEST"),
		StreamName:        aws.String(streamName),
	}

	resp, err := svc.GetShardIterator(params)
	if err != nil {
		panic(err)
	}

	return *resp.ShardIterator
}

func readShard(streamName string, streamShard kinesis.Shard, messageChannel chan kinesis.Record, region string) {
	svc := Kinesis(region)
	shardIterator := getShardIterator(streamName, streamShard, region)
	log.Debug("First shard iterator: ", shardIterator)

	for shardIterator != "" {
		params := &kinesis.GetRecordsInput{
			ShardIterator: aws.String(shardIterator),
			Limit:         aws.Int64(1000),
		}
		resp, err := svc.GetRecords(params)
		if err != nil {
			panic(err)
		}

		log.Debug(*streamShard.ShardId, " got ", len(resp.Records), " records")
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

func printRecords(messageChannel chan kinesis.Record) {
	for {
		select {
		case record := <-messageChannel:
			log.Debug("Record with partition-key: ", *record.PartitionKey, " and sequence number: ", *record.SequenceNumber)
			fmt.Println(string(record.Data[:]))
		}
	}
}

func readStream(streamName string, region string) {
	streamShards := getStreamShards(streamName, region)

	messageChannel := make(chan kinesis.Record, 100)
	var waitGroup sync.WaitGroup

	log.Debug("Stream has ", len(streamShards), " shard(s)")
	for _, streamShard := range streamShards {

		waitGroup.Add(1)
		log.Debug("Reading ", *streamShard.ShardId)
		go func(shard kinesis.Shard) {
			defer waitGroup.Done()
			readShard(streamName, shard, messageChannel, region)
		}(*streamShard)
	}

	printRecords(messageChannel)
	waitGroup.Wait()
}

func main() {
	var debug bool
	var region string

	runtime.GOMAXPROCS(runtime.NumCPU())

	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(os.Stderr)
	log.SetLevel(log.InfoLevel)

	app := cli.NewApp()
	app.EnableBashCompletion = true
	app.Name = "ktail"
	app.Usage = "Read json messages from AWS Kinesis streams"
	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:        "debug",
			Usage:       "Debug output",
			EnvVar:      "KTAIL_DEBUG",
			Destination: &debug,
		},
		cli.StringFlag{
			Name:        "region",
			Usage:       "specify the aws region",
			Value:       "us-east-1",
			EnvVar:      "AWS_REGION",
			Destination: &region,
		},
	}
	app.Action = func(c *cli.Context) {
		if debug == true {
			log.SetLevel(log.DebugLevel)
		}

		if len(c.Args()) == 0 {
			fmt.Println("streams in " + region + ":")
			for _, streamName := range getStreamNames(region) {
				fmt.Println(*streamName)
			}
		} else if len(c.Args()) > 0 {
			streamName := c.Args()[0]
			readStream(streamName, region)
		}
	}

	app.Run(os.Args)
}
