package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

const CREATE = false
const DESCRIBE = false
const PRODUCE = true
const CONSUME = false
const DELETE = false

var lastTimestampByShardId = map[string]time.Time{}

var stream = flag.String("stream", "PIM-product-info-dev", "")

func main() {
	fmt.Println("Starting...")
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String("ap-southeast-1"),
		Credentials: credentials.NewSharedCredentials("", "pim-kinesis"),
	}))
	// creds := stscreds.NewCredentials(sess, "arn:aws:iam::102991414667:role/r_aws-admin")
	// kc := kinesis.New(sess, &aws.Config{Credentials: creds})
	kc := kinesis.New(sess)
	streamName := aws.String(*stream)

	// Create Stream
	if CREATE {
		out, err := kc.CreateStream(&kinesis.CreateStreamInput{
			ShardCount: aws.Int64(1),
			StreamName: streamName,
		})
		if err != nil {
			panic(err)
		}
		fmt.Printf("%v\n", out)
	}

	// Connect Stream
	log.Println("Waiting stream...")
	if err := kc.WaitUntilStreamExists(&kinesis.DescribeStreamInput{StreamName: streamName}); err != nil {
		panic(err)
	}

	// Descibe
	if DESCRIBE {
		streams, err := kc.DescribeStream(&kinesis.DescribeStreamInput{StreamName: streamName})
		if err != nil {
			panic(err)
		}
		fmt.Printf("%v\n", streams)
	}

	// Produce Record
	var putOutput *kinesis.PutRecordOutput
	var err error
	if PRODUCE {
		now := time.Now().Local()
		putOutput, err = kc.PutRecord(&kinesis.PutRecordInput{
			Data:         []byte(now.Format(time.RFC3339)),
			StreamName:   streamName,
			PartitionKey: aws.String("PIM-product"),
		})
		if err != nil {
			panic(err)
		}
		fmt.Printf("%v\n", putOutput)
	}

	// Consume Records
	if CONSUME {
		for {
			listShardsOutput, err := kc.ListShards(&kinesis.ListShardsInput{
				StreamName: streamName,
			})
			if err != nil {
				panic(err)
			}
			// fmt.Printf("%v\n", listShardsOutput)

			for _, shard := range listShardsOutput.Shards {
				var iteratorOutput *kinesis.GetShardIteratorOutput
				var err error
				if lastTimestamp, ok := lastTimestampByShardId[*shard.ShardId]; ok {
					iteratorOutput, err = kc.GetShardIterator(&kinesis.GetShardIteratorInput{
						ShardId: shard.ShardId,
						// ShardIteratorType: aws.String("LATEST"),
						// ShardIteratorType: aws.String("TRIM_HORIZON"),
						// ShardIteratorType: aws.String("AFTER_SEQUENCE_NUMBER"),
						// ShardIteratorType:      aws.String("AT_SEQUENCE_NUMBER"),
						ShardIteratorType: aws.String("AT_TIMESTAMP"),
						Timestamp:         &lastTimestamp,
						StreamName:        streamName,
					})
				} else {
					iteratorOutput, err = kc.GetShardIterator(&kinesis.GetShardIteratorInput{
						ShardId: shard.ShardId,
						// ShardIteratorType: aws.String("LATEST"),
						ShardIteratorType: aws.String("TRIM_HORIZON"),
						StreamName:        streamName,
					})
				}
				if err != nil {
					panic(err)
				}
				// fmt.Printf("%v\n", *shard.ShardId)
				// fmt.Printf("%v\n", iteratorOutput)
				getRecords(kc, iteratorOutput.ShardIterator)
				lastTimestampByShardId[*shard.ShardId] = time.Now()
			}
			time.Sleep(1 * time.Second)
		}
	}

	// Delete
	if DELETE {
		deleteOutput, err := kc.DeleteStream(&kinesis.DeleteStreamInput{
			StreamName: streamName,
		})
		if err != nil {
			panic(err)
		}
		fmt.Printf("%v\n", deleteOutput)
	}
}

func getRecords(kc *kinesis.Kinesis, shardIterator *string) {
	records, err := kc.GetRecords(&kinesis.GetRecordsInput{
		ShardIterator: shardIterator,
	})
	if err != nil {
		panic(err)
	}
	// fmt.Printf("%v\n", records)
	fmt.Print(".")
	for _, record := range records.Records {
		fmt.Printf("\n%v\n", string(record.Data[:]))
		// fmt.Printf("%v\n", *record.SequenceNumber)
	}
	if *records.MillisBehindLatest != 0 && records.NextShardIterator != nil {
		getRecords(kc, records.NextShardIterator)
	}
}
