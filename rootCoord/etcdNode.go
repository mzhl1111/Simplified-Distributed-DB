package rootCord

import (
	"context"
	"errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
)

type EtcdNode struct {
	client *clientv3.Client
}

func (e *EtcdNode) saveToETCD(key string, value string) error {

	_, err := e.client.Put(context.Background(), key, value)
	return err
}

func (e *EtcdNode) getFromETCD(key string) ([]byte, error) {
	getResp, err := e.client.Get(context.Background(), key)
	if err != nil {
		log.Fatalf("Failed to retrieve key-value pair: %v", err)
	}
	if len(getResp.Kvs) > 0 {
		return getResp.Kvs[0].Value, nil
	} else {
		return nil, errors.New("empty value from ETCD")
	}
}
