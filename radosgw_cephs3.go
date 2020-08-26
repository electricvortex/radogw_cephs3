package s3api

import (
	"crypto/tls"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	goamz "github.com/goamz/goamz/aws"
	goamzs3"github.com/goamz/goamz/s3"
	"net/http"
	"os"
)

func s3Upload(accessKey, secretKey, endpoint, bucket string) error {
	auth := goamz.Auth{
		AccessKey: accessKey,
		SecretKey: secretKey,
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	connection := goamzs3.New(auth, goamz.EUWest, httpClient)
	connection.S3Endpoint = endpoint
	opBucket := connection.Bucket(bucket)

	file, err := os.Open("ez.png")
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		fmt.Println(err)
		return err
	}

	filesize := fileinfo.Size()
	buffer := make([]byte, filesize)

	_, err = file.Read(buffer)
	if err != nil {
		fmt.Println(err)
		return err
	}

	err = opBucket.Put("storm_test.png", buffer, "image/png", goamzs3.PublicRead, goamzs3.Options{})
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func s3NewUpload(accessKey, secretKey, sbucket, endpoint string) error {
	bucket := aws.String(sbucket)
	key := aws.String("storm_test.png")

	// Configure to use MinIO Server
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(accessKey, secretKey, ""),
		Endpoint:         aws.String(endpoint),
		Region:           aws.String("us-east-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}
	newSession, err := session.NewSession(s3Config)
	if err != nil {
		fmt.Println(err)
		return err
	}
	s3Client := s3.New(newSession)

	file, err := os.Open("ez.png")
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer file.Close()

	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Body: file,
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		fmt.Printf("Failed to upload data to %s/%s, %s\n", *bucket, *key, err.Error())
		return err
	}

	return nil
}
