package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

const CREATE = false
const DESCRIBE = true
const PRODUCE = false
const CONSUME = true
const DELETE = false

const LAST_SEQ_NUMER = "49592896276015172091846676797947732156385634425793150978"

var stream = flag.String("stream", "PIM-product-info-dev", "")

func main() {
	log.Println("Starting...")
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
		putOutput, err = kc.PutRecord(&kinesis.PutRecordInput{
			Data:         []byte("hoge"),
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
		iteratorOutput, err := kc.GetShardIterator(&kinesis.GetShardIteratorInput{
			ShardId: aws.String("shardId-000000000000"),
			// ShardIteratorType:      aws.String("AFTER_SEQUENCE_NUMBER"),
			// ShardIteratorType: aws.String("LATEST"),
			ShardIteratorType: aws.String("TRIM_HORIZON"),
			// StartingSequenceNumber: aws.String(LAST_SEQ_NUMER),
			StreamName: streamName,
		})
		if err != nil {
			panic(err)
		}
		fmt.Printf("%v\n", iteratorOutput)
		records, err := kc.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: iteratorOutput.ShardIterator,
		})

		if err != nil {
			panic(err)
		}
		// fmt.Printf("%v\n", records)
		for _, record := range records.Records {
			fmt.Printf("%v\n", string(record.Data[:]))
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
