package rootCord

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Core struct {
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	etcdCli      *EtcdNode
	tsoAllocator *TsAllocator
}

func (c *Core) tsLoop() {
	defer c.wg.Done()
	tsoTicker := time.NewTicker(50 * time.Millisecond)
	defer tsoTicker.Stop()
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	for {
		select {
		case <-tsoTicker.C:
			if err := c.tsoAllocator.UpdateTso(); err != nil {
				log.Printf("fail to update TSO")
				continue
			}

		case <-ctx.Done():
			log.Printf("rootcoord's ts loop quit!")
			return
		}
	}
}

func newCore(c context.Context) (*Core, error) {
	ctx, cancel := context.WithCancel(c)
	core := &Core{
		ctx: ctx, cancel: cancel,
	}
	return core, nil
}

func (c *Core) setEtcdClient(client *EtcdNode) {
	c.etcdCli = client
}

func (c *Core) InitTSOAllocator() error {
	tsoAllocator := NewTsAllocator("example_timestamp", c.etcdCli)
	if err := tsoAllocator.Initialize(); err != nil {
		return err
	}
	c.tsoAllocator = tsoAllocator

	log.Println("tso allocator initialized")

	return nil
}

func main() {
	mqUser := os.Getenv("MQ_USER")
	mqPass := os.Getenv("MQ_PASS")
	mqHost := os.Getenv("MQ_HOST")
	//mqUser := "guest"
	//mqPass := "guest"
	//mqHost := "localhost"

	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:5672/", mqUser, mqPass, mqHost))
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to connect to etcd: %v", err)
	}
	defer client.Close()

	etcdNode := &EtcdNode{client: client}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	core, err := newCore(ctx)
	if err != nil {
		panic(err.Error())
	}

	core.setEtcdClient(etcdNode)
	err = core.InitTSOAllocator()
	if err != nil {
		panic(err.Error())
	}

	go tsAllocateService(conn, core.tsoAllocator)

	select {}
}
