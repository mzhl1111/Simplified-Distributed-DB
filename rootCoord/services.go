package rootCord

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"strconv"
	"time"
)

func tsAllocateService(conn *amqp.Connection, allocator *TsAllocator) {
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		ts_queue, // name
		false,    // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for d := range msgs {
			n, err := strconv.Atoi(string(d.Body))
			failOnError(err, "Failed to convert body to integer")

			log.Printf(" [.] fib(%d)", n)
			response, err := allocator.GenerateTso(1)
			if err != nil {
				response = 0
			}

			err = ch.PublishWithContext(ctx,
				"",        // exchange
				d.ReplyTo, // routing key
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					Body:          []byte(strconv.FormatUint(response, 10)),
				})
			failOnError(err, "Failed to publish a message")

			d.Ack(false)
		}
	}()

	log.Printf(" [*] Awaiting TS allocate requests")
	<-forever
}

func s3DeleteFileService(conn *amqp.Connection, etcdNode EtcdNode) {
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"newImage", // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
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
	s := NewS3Node()
	var forever chan struct{}

	go func() {
		for d := range msgs {
			var msg DeleteMsg
			json.Unmarshal(d.Body, &msg)
			if msg.forceFail == "false" {
				err := s.deleteObject("s3-6650", fmt.Sprintf("%s.txt", msg.fileID))
				if err != nil {
					failOnError(err, "delete object failed")
				}
			}
			recByte, err := etcdNode.getFromETCD("files")
			if err != nil {
				failOnError(err, "fail to get map from etcd")
			}
			var record etcdFileTS
			json.Unmarshal(recByte, &record)

			delete(record.fileTsMap, msg.fileID)
			updatedByte, err := json.Marshal(record)
			if err != nil {
				failOnError(err, "marshal file ts map fail")
			}

			err = etcdNode.saveToETCD("files", string(updatedByte))
			if err != nil {
				failOnError(err, "save to etcd fail")
			}

			d.Ack(false)
		}
	}()

	log.Printf(" [*] Awaiting S3 delete requests")
	<-forever
}

func s3CreateFileService(conn *amqp.Connection, etcdNode *EtcdNode) {
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"newImage", // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
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
	s := NewS3Node()
	var forever chan struct{}

	go func() {
		for d := range msgs {
			var msg CreateMsg
			json.Unmarshal(d.Body, &msg)
			if msg.forceFail == "false" {
				content := strconv.FormatInt(msg.timeStamp, 10)

				// Convert string content to bytes.Buffer
				buffer := bytes.NewBufferString(content)
				err := s.putTextBuffer(buffer, "s3-6650", fmt.Sprintf("%s.txt", msg.fileID))
				if err != nil {
					failOnError(err, "delete object failed")
				}
			}

			recByte, err := etcdNode.getFromETCD("files")
			if err != nil {
				failOnError(err, "fail to get map from etcd")
			}
			var record etcdFileTS
			json.Unmarshal(recByte, &record)

			record.fileTsMap[msg.fileID] = msg.timeStamp
			updatedByte, err := json.Marshal(record)
			if err != nil {
				failOnError(err, "marshal file ts map fail")
			}

			err = etcdNode.saveToETCD("files", string(updatedByte))
			if err != nil {
				failOnError(err, "save to etcd fail")
			}

			d.Ack(false)
		}
	}()

	log.Printf(" [*] Awaiting S3 create requests")
	<-forever
}

func s3UpdateFileService(conn *amqp.Connection, etcdNode *EtcdNode) {
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"newImage", // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
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
	s := NewS3Node()
	var forever chan struct{}

	go func() {
		for d := range msgs {
			var msg UpdateMsg
			json.Unmarshal(d.Body, &msg)
			if msg.forceFail == "false" {
				content := strconv.FormatInt(msg.timeStamp, 10)

				// Convert string content to bytes.Buffer
				buffer := bytes.NewBufferString(content)
				err := s.putTextBuffer(buffer, "s3-6650", fmt.Sprintf("%s.txt", msg.fileID))
				if err != nil {
					failOnError(err, "delete object failed")
				}
			}
			recByte, err := etcdNode.getFromETCD("files")
			if err != nil {
				failOnError(err, "fail to get map from etcd")
			}
			var record etcdFileTS
			json.Unmarshal(recByte, &record)

			record.fileTsMap[msg.fileID] = msg.timeStamp
			updatedByte, err := json.Marshal(record)
			if err != nil {
				failOnError(err, "marshal file ts map fail")
			}

			err = etcdNode.saveToETCD("files", string(updatedByte))
			if err != nil {
				failOnError(err, "save to etcd fail")
			}

			d.Ack(false)
		}
	}()

	log.Printf(" [*] Awaiting S3 update requests")
	<-forever
}
