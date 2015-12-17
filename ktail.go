package main

import (
	"os"
	"fmt"
	"sort"
	"time"
	"runtime"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/codegangsta/cli"
	"encoding/json"
	"sync"
	log "github.com/Sirupsen/logrus"
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
		ShardIteratorType: aws.String("LATEST"),
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

func printRecords(messageChannel chan kinesis.Record, fields []string) {
	for {
		select {
		case record := <-messageChannel:
			log.Debug("Record with part-key: ", *record.PartitionKey, " and seq-nr: ", *record.SequenceNumber)

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
				var keys []string
				for k := range eventMap {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				for _, k := range keys {
					fmt.Print(k, "=", eventMap[k])
					fmt.Print("  ")
				}
				fmt.Println()
			}
		}
	}
}


func readStream(streamName string, fields []string) {
	streamShards := getStreamShards(streamName)

	messageChannel := make(chan kinesis.Record, 100)
	var waitGroup sync.WaitGroup

	log.Debug("Stream has ", len(streamShards), " shard(s)")
	for _, streamShard := range streamShards {

		waitGroup.Add(1)
		log.Debug("Reading ", *streamShard.ShardId)
		go func(shard kinesis.Shard) {
			defer waitGroup.Done()
			readShard(streamName, shard, messageChannel)
		}(*streamShard)
	}

	printRecords(messageChannel, fields)
	waitGroup.Wait()
}

func main() {
	var debug bool
	runtime.GOMAXPROCS(runtime.NumCPU())

	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(os.Stderr)
	log.SetLevel(log.InfoLevel)

	app := cli.NewApp()
	app.EnableBashCompletion = true
	app.Name = "ktail"
	app.Usage = "Read json messages from AWS Kinesis streams"
	app.Flags = []cli.Flag{
		cli.StringSliceFlag{
			Name: "field",
			Usage: "define field to print instead of the complete message",
			EnvVar: "KTAIL_FIELDS",
		},
		cli.BoolFlag{
			Name: "debug",
			Usage: "Debug output",
			EnvVar: "KTAIL_DEBUG",
			Destination: &debug,
		},
	}
	app.Action = func(c *cli.Context) {
		if debug == true {
			log.SetLevel(log.DebugLevel)
		}

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

