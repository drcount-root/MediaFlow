package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"

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
	return &MinIOStorage{client: client, rawBucket: rawBucket, processedBucket: processedBucket, thumbnailBucket: thumbnailBucket}, nil
}

func (s *MinIOStorage) DownloadRaw(ctx context.Context, objectKey, destination string) error {
	object, err := s.client.GetObject(ctx, s.rawBucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer object.Close()

	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}

	file, err := os.Create(destination)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, object)
	return err
}

func (s *MinIOStorage) UploadProcessedFile(ctx context.Context, objectKey, path, contentType string) error {
	return s.uploadFile(ctx, s.processedBucket, objectKey, path, contentType)
}

func (s *MinIOStorage) UploadThumbnail(ctx context.Context, objectKey, path string) error {
	return s.uploadFile(ctx, s.thumbnailBucket, objectKey, path, "image/jpeg")
}

func (s *MinIOStorage) uploadFile(ctx context.Context, bucket, objectKey, path, contentType string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	_, err = s.client.PutObject(ctx, bucket, objectKey, file, stat.Size(), minio.PutObjectOptions{
		ContentType: contentType,
	})
	return err
}
