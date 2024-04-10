package client

import (
	"encoding/json"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

func listenLamportProcess(conn *amqp.Connection, ts *LamportTs) {
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"LamportBroadCast", // name
		false,              // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")
	var forever chan struct{}

	go func() {
		for d := range msgs {
			var msg BaseMsg
			json.Unmarshal(d.Body, &msg)
			ts.ReceiveMessage(msg.timeStamp)
		}
	}()

	log.Printf(" [*] Awaiting RPC S3 requests")
	<-forever
}
