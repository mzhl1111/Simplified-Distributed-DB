package dataCoord

import (
	"context"
	"encoding/json"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

type dataCoordServer struct {
	conn *amqp.Connection
	core *DataCoordCore
}

func newDataCoordServer(conn *amqp.Connection, core *DataCoordCore) *dataCoordServer {
	newDcs := &dataCoordServer{
		conn: conn,
		core: core,
	}
	return newDcs
}

func (dcs *dataCoordServer) s3DeleteFileService() {
	ch, err := dcs.conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"deleteFile", // name
		false,        // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
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
			var msg DeleteMsg
			json.Unmarshal(d.Body, &msg)
			fileMeta, ok := dcs.core.filemeta.Read(msg.fileID)
			if !ok {
				log.Printf("%s not exist, delete fail", msg.fileID)
			} else {
				fileMeta.Properties[Status] = DeletePending
				dcs.core.filemeta.Update(msg.fileID, fileMeta)
				if msg.forceFail == "true" {
				} else {
					fileMeta.Properties[Status] = Deleting
					dcs.core.filemeta.Update(msg.fileID, fileMeta)

					dcs.core.binLog.Unlock()
					time.Sleep(time.Second)
					fileMeta.Properties[Status] = Deleted
					dcs.core.binLog.Delete(DataStore, msg.fileID)
					dcs.core.binLog.Lock()

					dcs.core.filemeta.Update(msg.fileID, fileMeta)
				}
			}
			d.Ack(false)
		}
	}()

	log.Printf(" [*] Awaiting delete requests")
	<-forever
}

func (dcs *dataCoordServer) CreateFileService() {
	ch, err := dcs.conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"newFile", // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
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
			var msg CreateMsg
			json.Unmarshal(d.Body, &msg)
			fileMeta, ok := dcs.core.filemeta.Read(msg.fileID)
			if ok {
				log.Printf("%s already exist, create fail", msg.fileID)
			} else {
				fileMeta.Properties[Status] = CreatePending
				dcs.core.filemeta.Create(msg.fileID, fileMeta)
				if msg.forceFail == "true" {
				} else {
					fileMeta.Properties[Status] = Creating
					dcs.core.filemeta.Update(msg.fileID, fileMeta)

					dcs.core.binLog.Unlock()
					time.Sleep(time.Second)
					fileMeta.Properties[Status] = Created
					dcs.core.binLog.Create(DataStore, msg.fileID, msg.data)
					dcs.core.binLog.Lock()

					dcs.core.filemeta.Update(msg.fileID, fileMeta)
				}
			}
			d.Ack(false)
		}
	}()

	log.Printf(" [*] Awaiting S3 create requests")
	<-forever
}

func (dcs *dataCoordServer) UpdateFileService() {
	ch, err := dcs.conn.Channel()
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
	var forever chan struct{}

	go func() {
		for d := range msgs {
			var msg UpdateMsg
			json.Unmarshal(d.Body, &msg)
			fileMeta, ok := dcs.core.filemeta.Read(msg.fileID)
			if !ok {
				log.Printf("%s not exist, update fail", msg.fileID)
			} else {
				fileMeta.Properties[Status] = CreatePending
				dcs.core.filemeta.Update(msg.fileID, fileMeta)
				if msg.forceFail == "true" {
				} else {
					fileMeta.Properties[Status] = Creating
					dcs.core.filemeta.Update(msg.fileID, fileMeta)

					dcs.core.binLog.Unlock()
					time.Sleep(time.Second)
					fileMeta.Properties[Status] = Created
					dcs.core.binLog.Update(DataStore, msg.fileID, msg.data)
					dcs.core.binLog.Lock()

					dcs.core.filemeta.Update(msg.fileID, fileMeta)
				}
			}
			d.Ack(false)
		}
	}()

	log.Printf(" [*] Awaiting S3 update requests")
	<-forever
}

func (dcs *dataCoordServer) GetFileService() {
	ch, err := dcs.conn.Channel()
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
	var forever chan struct{}

	go func() {
		var resp []byte
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		for d := range msgs {
			var msg GetMsg
			json.Unmarshal(d.Body, &msg)
			for msg.timeStamp > dcs.core.tsMeta.Read(ServiceTs) {
				time.Sleep(10 * time.Millisecond)
			}
			fileMeta, ok := dcs.core.filemeta.Read(msg.fileID)
			if !ok {
				log.Printf("%s not exist, get fail\n", msg.fileID)
			} else {
				fileMeta.Properties[Status] = CreatePending
				dcs.core.filemeta.Update(msg.fileID, fileMeta)
				if msg.forceFail == "true" {
				} else {
					fileMeta.Properties[Status] = Creating
					dcs.core.filemeta.Update(msg.fileID, fileMeta)

					dcs.core.binLog.Unlock()
					time.Sleep(time.Second)
					fileMeta.Properties[Status] = Created
					data, dataTs, ok := dcs.core.binLog.Read(DataStore, msg.fileID)
					dcs.core.binLog.Lock()
					if !ok {
						diskBinLog, err := dcs.core.ImportBinLog()
						if err != nil {
							log.Println("Import Binlog failed")
						}
						data, ok = diskBinLog[DataStore][msg.fileID]
						if !ok {
							log.Printf("%s not in binlog", msg.fileID)
							break
						}
					}
					if dataTs < msg.timeStamp {
						log.Println("get ts valid")
					}
					resp = data
				}
			}
			err = ch.PublishWithContext(ctx,
				"",        // exchange
				d.ReplyTo, // routing key
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType:   "application/octet-stream",
					CorrelationId: d.CorrelationId,
					Body:          resp,
				})
			failOnError(err, "Failed to publish a message")
			d.Ack(false)
		}
	}()

	log.Printf(" [*] Awaiting S3 update requests")
	<-forever
}
