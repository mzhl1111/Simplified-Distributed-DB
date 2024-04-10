package rootCord

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
)

type S3Node struct {
	client *s3.S3
}

func NewS3Node() *S3Node {
	awsRegion := os.Getenv("AWS_REGION")
	awsAccessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	awsSecretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	awsSessionToken := os.Getenv("AWS_SESSION_TOKEN")

	awsSession, err := session.NewSession(&aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, awsSessionToken),
	})
	if err != nil {
		panic("Failed to initialize AWS session:")
	}
	s3Client := s3.New(awsSession)

	return &S3Node{s3Client}
}

func (s *S3Node) putObject(bytesObj []byte, bucketName string, key string) error {
	_, err := s.client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader(bytesObj),
	})
	return err
}

func (s *S3Node) putTextBuffer(buffer *bytes.Buffer, bucketName string, key string) error {
	_, err := s.client.PutObject(&s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(key),
		Body:        bytes.NewReader(buffer.Bytes()),
		ContentType: aws.String("text/plain"),
	})
	return err
}

func (s *S3Node) deleteObject(bucketName string, key string) error {
	_, err := s.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	return err
}
