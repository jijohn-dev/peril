package main

import (
	"fmt"

	"github.com/jijohn-dev/peril/internal/gamelogic"
	"github.com/jijohn-dev/peril/internal/pubsub"
	"github.com/jijohn-dev/peril/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	connectionString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Printf("error connecting to RabbitMQ: %s", err)
		return
	}
	defer conn.Close()

	channel, err := conn.Channel()

	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.SimpleQueueDurable,
		handlerLog(),
	)

	if err != nil {
		fmt.Printf("error subscribing to game logs: %s\n", err)
	}

	fmt.Println("Connected to RabbitMQ")
	gamelogic.PrintServerHelp()

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		if input[0] == "pause" {
			fmt.Println("sending pause message")
			pubsub.PublishJSON(channel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)
		} else if input[0] == "resume" {
			fmt.Println("sending resume message")
			pubsub.PublishJSON(channel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
		} else if input[0] == "quit" {
			fmt.Println("Shutting down...")
			break
		} else {
			fmt.Println("unrecognized command")
		}
	}
}
