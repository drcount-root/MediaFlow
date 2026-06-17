package storage

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/minio/minio-go/v7"
	"mediaflow/apps/api/internal/uploads"
)

// Milestone 6: presigned multipart ingest. These methods drive MinIO's low-level
// multipart API (via minio.Core) so the browser can upload parts directly to the
// raw bucket without the bytes passing through the API process.

func (s *MinIOStorage) core() minio.Core {
	return minio.Core{Client: s.client}
}

// InitiateMultipart starts a multipart upload and returns its upload id.
func (s *MinIOStorage) InitiateMultipart(ctx context.Context, objectKey, contentType string) (string, error) {
	return s.core().NewMultipartUpload(ctx, s.rawBucket, objectKey, minio.PutObjectOptions{
		ContentType: contentType,
	})
}

// PresignPartURL returns a presigned PUT URL the client uses to upload one part
// directly to object storage.
func (s *MinIOStorage) PresignPartURL(ctx context.Context, objectKey, uploadID string, partNumber int, expires time.Duration) (string, error) {
	params := url.Values{}
	params.Set("uploadId", uploadID)
	params.Set("partNumber", strconv.Itoa(partNumber))

	u, err := s.client.Presign(ctx, http.MethodPut, s.rawBucket, objectKey, expires, params)
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

// ListParts returns the parts object storage has already received for a multipart
// upload, paging through the results.
func (s *MinIOStorage) ListParts(ctx context.Context, objectKey, uploadID string) ([]uploads.UploadedPart, error) {
	var parts []uploads.UploadedPart
	marker := 0
	for {
		res, err := s.core().ListObjectParts(ctx, s.rawBucket, objectKey, uploadID, marker, 1000)
		if err != nil {
			return nil, err
		}
		for _, p := range res.ObjectParts {
			parts = append(parts, uploads.UploadedPart{
				PartNumber: p.PartNumber,
				ETag:       p.ETag,
				Size:       p.Size,
			})
		}
		if !res.IsTruncated {
			break
		}
		marker = res.NextPartNumberMarker
	}
	return parts, nil
}

// CompleteMultipart finalizes a multipart upload by concatenating the named
// parts (in order) into the object.
func (s *MinIOStorage) CompleteMultipart(ctx context.Context, objectKey, uploadID string, parts []uploads.CompletePart) error {
	cp := make([]minio.CompletePart, len(parts))
	for i, p := range parts {
		cp[i] = minio.CompletePart{PartNumber: p.PartNumber, ETag: p.ETag}
	}
	_, err := s.core().CompleteMultipartUpload(ctx, s.rawBucket, objectKey, uploadID, cp, minio.PutObjectOptions{})
	return err
}

// AbortMultipart releases an in-progress multipart upload and its staged parts.
func (s *MinIOStorage) AbortMultipart(ctx context.Context, objectKey, uploadID string) error {
	return s.core().AbortMultipartUpload(ctx, s.rawBucket, objectKey, uploadID)
}
