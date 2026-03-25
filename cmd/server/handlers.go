package main

import (
	"fmt"

	"github.com/jijohn-dev/peril/internal/gamelogic"
	"github.com/jijohn-dev/peril/internal/pubsub"
	"github.com/jijohn-dev/peril/internal/routing"
)

func handlerLog() func(log routing.GameLog) pubsub.AckType {
	return func(log routing.GameLog) pubsub.AckType {
		defer fmt.Println("> ")
		err := gamelogic.WriteLog(log)
		if err != nil {
			fmt.Println("unable to write log file")
			return pubsub.NackDiscard
		}
		return pubsub.Ack
	}
}
