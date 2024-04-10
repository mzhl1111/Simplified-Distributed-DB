package client

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"math/rand"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

type RabbitMQClient struct {
	conn      *amqp.Connection
	queueName string
	rspQName  string
	ch        chan []byte
	rspCh     chan string
	Lamport   *LamportTs
}

func NewRabbitMQClient(conn *amqp.Connection, queueName string, rspQName string, Lamport *LamportTs) *RabbitMQClient {
	return &RabbitMQClient{
		conn:      conn,
		queueName: queueName,
		rspQName:  rspQName,
		ch:        make(chan []byte, 500),
		rspCh:     make(chan string, 500),
		Lamport:   Lamport,
	}
}

func (r *RabbitMQClient) startRpc(n int) {
	for i := 0; i < n; i++ {
		go func() {
			for msg := range r.ch {
				if err := r.rpcCall(msg); err != nil {
					log.Printf("Failed to send message to RabbitMQ: %v", err)
				}
			}
		}()
	}
}

func (r *RabbitMQClient) startMsg(n int) {
	for i := 0; i < n; i++ {
		go func() {
			for msg := range r.ch {
				if err := r.sendMsg(msg); err != nil {
					log.Printf("Failed to send message to RabbitMQ: %v", err)
				}
			}
		}()
	}
}

func (r *RabbitMQClient) sendMsg(msg []byte) error {
	ch, err := r.conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx,
		"",          // exchange
		r.queueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			ReplyTo:     "no",
			Body:        msg,
		})
	failOnError(err, "Failed to publish a message")

	return nil
}

func (r *RabbitMQClient) rpcCall(msg []byte) error {
	ch, err := r.conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		r.rspQName, // name
		false,      // durable
		false,      // delete when unused
		true,       // exclusive
		false,      // noWait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	corrId := randomString(32)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx,
		"",          // exchange
		r.queueName, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       r.rspQName,
			Body:          msg,
		})
	failOnError(err, "Failed to publish a message")

	for d := range msgs {
		if corrId == d.CorrelationId {
			r.rspCh <- string(d.Body)
			failOnError(err, "Failed to convert body to integer")
			break
		}
	}

	return nil
}
