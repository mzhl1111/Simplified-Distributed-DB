package rootCord

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3v2 "github.com/aws/aws-sdk-go-v2/service/s3"
	clientv3 "go.etcd.io/etcd/client/v3"
	"io"
	"log"
	"strconv"
	"strings"
	"time"
)

type garbageCollector struct {
	s3v2client *s3v2.Client
	etcdNode   *EtcdNode
	delete1ch  chan []byte
	ch         chan []byte
}

func newGarbageCollector() *garbageCollector {
	accessKeyID := "your-access-key-id"
	secretAccessKey := "your-secret-access-key"
	sessionToken := "your-session-token" // This can be an empty string if you're not using temporary credentials

	// Create custom AWS credentials
	customCreds := credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, sessionToken)

	// Load AWS configuration with custom credentials
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(customCreds),
	)
	if err != nil {
		panic("configuration error, " + err.Error())
	}

	client := s3v2.NewFromConfig(cfg)

	etcdcli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to connect to etcd: %v", err)
	}

	return &garbageCollector{s3v2client: client, etcdNode: &EtcdNode{client: etcdcli}}
}

func (g *garbageCollector) startTimer() {
	go func() {
		timer := time.NewTimer(100 * time.Millisecond)

		// Wait for the timer to expire.
		<-timer.C

		g.cleanMisMatch()
	}()
}

func (g *garbageCollector) cleanMisMatch() {
	s3map := g.GetFullRecordFromS3()
	recByte, err := g.etcdNode.getFromETCD("files")
	if err != nil {
		failOnError(err, "fail to get map from etcd")
	}
	var record etcdFileTS
	json.Unmarshal(recByte, &record)

	for fileID, ts := range s3map {
		etcdTs, ok := record.fileTsMap[fileID]
		// If the key exists
		if ok {
			if ts == etcdTs {
				continue
			} else {
				log.Printf("file %s ts mismatch etch: %d, s3: %d\n", fileID, etcdTs, ts)
			}
		} else {
			g.deleteFile(fmt.Sprintf("%s.txt", fileID))
		}
	}
}

func (g *garbageCollector) GetFullRecordFromS3() map[string]int64 {
	var bucketName = "s3-6650"
	paginator := s3v2.NewListObjectsV2Paginator(g.s3v2client, &s3v2.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})

	res := make(map[string]int64)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			fmt.Println("Failed to list objects:", err)
			return nil
		}

		for _, obj := range page.Contents {
			if strings.HasSuffix(*obj.Key, ".txt") {
				// Get the object
				output, err := g.s3v2client.GetObject(context.TODO(), &s3v2.GetObjectInput{
					Bucket: aws.String(bucketName),
					Key:    obj.Key,
				})
				if err != nil {
					fmt.Println("Failed to get object:", err)
					continue
				}

				// Read the content of the file
				buf := new(strings.Builder)
				_, err = io.Copy(buf, output.Body)
				if err != nil {
					fmt.Println("Failed to read object body:", err)
					continue
				}
				output.Body.Close()

				// Convert content to int64
				number, err := strconv.ParseInt(strings.TrimSpace(buf.String()), 10, 64)
				if err != nil {
					fmt.Println("Failed to parse content:", err)
					continue
				}

				// Extract the filename without extension and store the data
				filename := strings.TrimSuffix(*obj.Key, ".txt")
				res[filename] = number
			}
		}
	}

	return res
}

func (g *garbageCollector) deleteFile(fileName string) error {
	var bucketName = "s3-6650"
	_, err := g.s3v2client.DeleteObject(context.TODO(), &s3v2.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(fileName),
	})
	return err
}
