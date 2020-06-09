package main

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// GetLogEvents retrieves CloudWatchLog events.
// Inputs:
//     sess is the current session, which provides configuration for the SDK's service clients
//     limit is the maximum number of log events to retrieve
//     logGroupName is the name of the log group
//     logStreamName is the name of the log stream
// Output:
//     If success, a GetLogEventsOutput object containing the events and nil
//     Otherwise, nil and an error from the call to GetLogEvents
func GetLogEvents(sess *session.Session, logGroupName *string, logStreamName *string, nextToken *string) (*cloudwatchlogs.GetLogEventsOutput, error) {
	svc := cloudwatchlogs.New(sess)

	resp, err := svc.GetLogEvents(&cloudwatchlogs.GetLogEventsInput{
		LogGroupName:  logGroupName,
		LogStreamName: logStreamName,
		Limit:         aws.Int64(1000),
		NextToken:     nextToken,
	})
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func GetLogStream(sess *session.Session, logGroupName *string) (*cloudwatchlogs.LogStream, error) {
	svc := cloudwatchlogs.New(sess)

	resp, err := svc.DescribeLogStreams(&cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName:        logGroupName,
		Descending:          aws.Bool(true),
		LogStreamNamePrefix: aws.String("main_main"),
	})
	if err != nil {
		return nil, err
	}

	return resp.LogStreams[0], nil
}

func main() {
	logGroupName := aws.String("/headspace/HSAWEBPROD01")

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
		"client.id":         "cwlogs",
		"auto.offset.reset": "earliest",
	})

	ls, err := GetLogStream(sess, logGroupName)
	if err != nil {
		log.Fatal(err)
	}

	resp, err := GetLogEvents(sess, logGroupName, ls.LogStreamName, nil)
	if err != nil {
		fmt.Println("Got error getting log events:")
		fmt.Println(err)
		return
	}

	fmt.Println("Event messages for stream " + *ls.LogStreamName + " in log group  " + *logGroupName)

	gotToken := ""
	nextToken := ""

	for true {

		gotToken = nextToken
		nextToken = *resp.NextForwardToken

		if gotToken == nextToken {
			fmt.Print("Waiting 5 sec")
			<-time.After(5 * time.Second)
			resp, err = GetLogEvents(sess, logGroupName, ls.LogStreamName, resp.NextForwardToken)
			if err != nil {
				fmt.Println("Got error getting log events:")
				fmt.Println(err)
				return
			}
		}

		for i, event := range resp.Events {

			//fmt.Println("  ", *event.Message)
			var logEntry Logs
			err = json.Unmarshal([]byte(*event.Message), &logEntry)
			if err != nil {
				// Ignore non-matching JSON
				continue
			}

			//js := json.NewEncoder(os.Stdout)
			//js.SetIndent("", "  ")
			//_ = js.Encode(logEntry)

			msg, _ := json.Marshal(logEntry)

			fmt.Printf("Produce: %d\n", i)
			delivery_chan := make(chan kafka.Event, 10000)
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     aws.String("logs"),
					Partition: kafka.PartitionAny,
				},
				Value:     msg,
				Key:       []byte(logEntry.Time),
				Timestamp: time.Time{},
			}, delivery_chan)
			if err != nil {
				log.Fatal(err)
			}

			e := <-delivery_chan
			m := e.(*kafka.Message)
			fmt.Printf("Resp: %s\n", m.Key)
		}
		resp, err = GetLogEvents(sess, logGroupName, ls.LogStreamName, resp.NextForwardToken)
		if err != nil {
			fmt.Println("Got error getting log events:")
			fmt.Println(err)
			return
		}

	}
}

type Logs struct {
	Time        string `json:"time"`
	RemoteIP    string `json:"remoteIP"`
	Host        string `json:"host"`
	RequestPath string `json:"requestPath"`
	Query       string `json:"query"`
	Method      string `json:"method"`
	Status      int    `json:"status"`
	UserAgent   string `json:"userAgent"`
	Referer     string `json:"referer"`
	Application string `json:"application"`
	Logtype     string `json:"logtype"`
}
