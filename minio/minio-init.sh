#!/bin/sh
set -e

echo "Waiting for MinIO..."
until mc alias set myminio http://minio-service:9000 minioadmin minioadmin; do
  sleep 2
done

echo "Creating bucket..."
mc mb --ignore-existing myminio/my-bucket

echo "Removing existing webhook notification (if any)..."
mc event remove myminio/my-bucket arn:minio:sqs::primary:webhook || true

echo "Adding webhook notification..."
mc event add myminio/my-bucket arn:minio:sqs::primary:webhook --event put

echo "MinIO initialization complete."
