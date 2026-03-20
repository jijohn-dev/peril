package main

import (
	"fmt"

	"github.com/jijohn-dev/peril/internal/gamelogic"
	"github.com/jijohn-dev/peril/internal/pubsub"
	"github.com/jijohn-dev/peril/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connectionString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Printf("error connecting to RabbitMQ: %s", err)
		return
	}
	defer conn.Close()

	fmt.Println("Connected to RabbitMQ")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("error setting username: %s", err)
		return
	}

	state := gamelogic.NewGameState(username)

	queueName := routing.PauseKey + "." + state.GetUsername()

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(state),
	)
	if err != nil {
		fmt.Printf("could not subscribe to pause: %s", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+state.GetUsername(),
		routing.ArmyMovesPrefix+".*",
		pubsub.SimpleQueueTransient,
		handlerArmyMoves(state),
	)

	ch, err := conn.Channel()
	if err != nil {
		fmt.Printf("error opening channe: %s", err)
		return
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "spawn":
			err := state.CommandSpawn(input)
			if err != nil {
				fmt.Printf("error: %s", err)
			}
		case "move":
			move, err := state.CommandMove(input)
			if err != nil {
				fmt.Printf("error: %s", err)
			}
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+"."+state.GetUsername(),
				move,
			)
			if err != nil {
				fmt.Printf("error: %s", err)
			} else {
				fmt.Println("move published")
			}
		case "status":
			state.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("unrecongnized command")
		}
	}
}
