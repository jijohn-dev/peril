package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, fmt.Errorf("Error connecting to RabbitMQ: %s", err)
	}

	queue, err := ch.QueueDeclare(
		queueName,                         // name
		queueType == SimpleQueueDurable,   // durable
		queueType == SimpleQueueTransient, // delete when unused
		queueType == SimpleQueueTransient, // exclusive
		false,                             // no-wait
		nil,                               // args
	)
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, fmt.Errorf("Error declaring queue: %s", err)
	}

	err = ch.QueueBind(
		queue.Name, // queue name
		key,        // routing key
		exchange,   // exchange
		false,      // no-wait
		nil,        // args
	)

	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, fmt.Errorf("Error binding queue: %s", err)
	}

	return ch, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	deliveryChannel, err := ch.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return err
	}

	go func() {
		defer ch.Close()
		for message := range deliveryChannel {
			var data T
			json.Unmarshal(message.Body, &data)
			handler(data)
			message.Ack(false)
		}
	}()

	return nil
}
