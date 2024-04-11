package dataCoord

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

type DataCoordCore struct {
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	binLog   BinLog
	filemeta Meta
	tsMeta   TsMeta
	tsMq     *RabbitMQClient
}

const (
	ServiceTs = "serviceTs"
)

func NewDataCoordCore(c context.Context, conn *amqp.Connection) *DataCoordCore {
	ctx, cancel := context.WithCancel(c)
	return &DataCoordCore{
		ctx:      ctx,
		cancel:   cancel,
		binLog:   *NewBinLog(),
		filemeta: *NewMeta(ctx),
		tsMeta:   *NewTsMeta(ctx),
		tsMq:     NewRabbitMQClient(conn, tsAllocateQueue, tsAllocateResponseQueue),
	}
}

func (dcc *DataCoordCore) ServiceTsLoop() {
	defer dcc.wg.Done()
	tsoTicker := time.NewTicker(100 * time.Millisecond)
	defer tsoTicker.Stop()
	ctx, cancel := context.WithCancel(dcc.ctx)
	defer cancel()
	for {
		select {
		case <-tsoTicker.C:
			buf := new(bytes.Buffer)
			err := binary.Write(buf, binary.LittleEndian, 1)
			if err != nil {
				log.Printf("ServiceTs get failed")
				continue
			}
			err = dcc.tsMq.rpcCall(buf.Bytes())
			if err != nil {
				log.Printf("ServiceTs get failed")
				continue
			}

			tsBin := <-dcc.tsMq.rspCh
			ts, err := strconv.ParseUint(string(tsBin), 10, 64)
			if err != nil {
				log.Printf("ServiceTs get failed")
			}
			dcc.tsMeta.Upsert(ServiceTs, ts)
			continue

		case <-ctx.Done():
			log.Printf("serviceTs's ts loop quit!")
			return
		}
	}
}

func (dcc *DataCoordCore) ImportBinLog() (map[string]map[string][]byte, error) {
	res := make(map[string]map[string][]byte)
	return res, nil
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

	ctx := context.Background()
	dcc := NewDataCoordCore(ctx, conn)

	dcc.ServiceTsLoop()

}
