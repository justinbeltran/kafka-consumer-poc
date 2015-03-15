package main

import (
	"fmt"
	gkc "github.com/stealthly/go_kafka_client"
)

func main() {
	//Channel for messages
	msgs := make(chan string)
	config := gkc.DefaultConsumerConfig()

	//Sets strategy for dealing with messages. Adds messages as string to msgs channel
	config.Strategy = func(worker *gkc.Worker, msg *gkc.Message, taskId gkc.TaskId) gkc.WorkerResult {
		msgs <- string(msg.Value)
		return gkc.NewSuccessfulResult(taskId)
	}

	//Set failure callbacks
	config.WorkerFailureCallback = func(*gkc.WorkerManager) gkc.FailedDecision {
		return gkc.CommitOffsetAndContinue
	}
	config.WorkerFailedAttemptCallback = func(*gkc.Task, gkc.WorkerResult) gkc.FailedDecision {
		return gkc.CommitOffsetAndContinue
	}

	//Build new consumer from configuration
	consumer := gkc.NewConsumer(config)
	defer consumer.Close()

	//Start consuming for "testing" topic
	topic := "test"
	go consumer.StartStatic(map[string]int{
		topic: 1,
	})

	//Print out messages from msg channel from "testing" topic
	for {
		select {
		case msg := <-msgs:
			fmt.Println(msg)
		}
	}
}
