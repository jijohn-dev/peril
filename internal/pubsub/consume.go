package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
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
		amqp.Table{"x-dead-letter-exchange": "peril_dlx"}, // args
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
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	err = ch.Qos(10, 0, false)
	if err != nil {
		return fmt.Errorf("could not set QoS: %v", err)
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
		for msg := range deliveryChannel {
			var data T
			json.Unmarshal(msg.Body, &data)
			ack := handler(data)
			switch ack {
			case Ack:
				msg.Ack(false)
				fmt.Println("message ack'd")
			case NackRequeue:
				msg.Nack(false, true)
				fmt.Println("message nack'd and requeued")
			case NackDiscard:
				msg.Nack(false, false)
				fmt.Println("message nack'd and discarded")
			}
		}
	}()

	return nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	err = ch.Qos(10, 0, false)
	if err != nil {
		return fmt.Errorf("could not set QoS: %v", err)
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
		for msg := range deliveryChannel {
			var data T
			buf := bytes.NewBuffer(msg.Body)
			dec := gob.NewDecoder(buf)
			err := dec.Decode(&data)
			if err != nil {
				fmt.Println("could not decode message")
			}

			ack := handler(data)
			switch ack {
			case Ack:
				msg.Ack(false)
				fmt.Println("message ack'd")
			case NackRequeue:
				msg.Nack(false, true)
				fmt.Println("message nack'd and requeued")
			case NackDiscard:
				msg.Nack(false, false)
				fmt.Println("message nack'd and discarded")
			}
		}
	}()

	return nil
}
