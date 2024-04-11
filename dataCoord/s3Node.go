package dataCoord

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3v2 "github.com/aws/aws-sdk-go-v2/service/s3"
	"log"
	"os"
)

const (
	default_bucket = "s3-6650"
)

type S3Node struct {
	client *s3v2.Client
}

func NewS3Node() *S3Node {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(os.Getenv("AWS_REGION")),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			os.Getenv("AWS_ACCESS_KEY_ID"),
			os.Getenv("AWS_SECRET_ACCESS_KEY"),
			os.Getenv("AWS_SESSION_TOKEN"))),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// Create an Amazon S3 service client
	client := s3v2.NewFromConfig(cfg)

	return &S3Node{
		client: client,
	}
}

func (s *S3Node) PutData(ctx context.Context, bucket, key string, data map[string][]byte) error {
	// Convert the data map to a JSON string
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// Put the object in the specified S3 bucket
	_, err = s.client.PutObject(ctx, &s3v2.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(jsonData),
	})

	return err
}

func (s *S3Node) GetData(ctx context.Context, bucket, key string) (map[string][]byte, error) {
	result, err := s.client.GetObject(ctx, &s3v2.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer result.Body.Close()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(result.Body)
	if err != nil {
		return nil, err
	}

	data := make(map[string][]byte)
	err = json.Unmarshal(buf.Bytes(), &data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (s *S3Node) DeleteData(ctx context.Context, bucket, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3v2.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	return err
}

func (s *S3Node) listObjectsWithPrefix(bucketName, prefix string) ([]string, error) {
	// Create the input for ListObjectsV2 operation
	input := &s3v2.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	}

	var objectKeys []string

	// Paginate through the list of objects and add their keys to objectKeys slice
	paginator := s3v2.NewListObjectsV2Paginator(s.client, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(context.TODO())
		if err != nil {
			return nil, fmt.Errorf("failed to list objects for page, %w", err)
		}

		for _, object := range output.Contents {
			objectKeys = append(objectKeys, *object.Key)
		}
	}

	return objectKeys, nil
}
