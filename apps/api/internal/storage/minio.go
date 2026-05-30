package storage

import (
	"context"
	"io"
	"net/url"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinIOStorage struct {
	client          *minio.Client
	rawBucket       string
	processedBucket string
	thumbnailBucket string
}

func NewMinIOStorage(endpoint, accessKey, secretKey string, useSSL bool, rawBucket, processedBucket, thumbnailBucket string) (*MinIOStorage, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, err
	}

	return &MinIOStorage{
		client:          client,
		rawBucket:       rawBucket,
		processedBucket: processedBucket,
		thumbnailBucket: thumbnailBucket,
	}, nil
}

func (s *MinIOStorage) UploadRaw(ctx context.Context, objectKey string, body io.Reader, size int64, contentType string) error {
	_, err := s.client.PutObject(ctx, s.rawBucket, objectKey, body, size, minio.PutObjectOptions{
		ContentType: contentType,
	})
	return err
}

func (s *MinIOStorage) PresignedProcessedURL(ctx context.Context, objectKey string, expires time.Duration) (string, error) {
	return s.presignedGetURL(ctx, s.processedBucket, objectKey, expires)
}

func (s *MinIOStorage) PresignedThumbnailURL(ctx context.Context, objectKey string, expires time.Duration) (string, error) {
	return s.presignedGetURL(ctx, s.thumbnailBucket, objectKey, expires)
}

func (s *MinIOStorage) presignedGetURL(ctx context.Context, bucket, objectKey string, expires time.Duration) (string, error) {
	u, err := s.client.PresignedGetObject(ctx, bucket, objectKey, expires, url.Values{})
	if err != nil {
		return "", err
	}

	return u.String(), nil
}
