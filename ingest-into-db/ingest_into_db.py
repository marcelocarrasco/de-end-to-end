from minio import Minio
from minio.error import S3Error

# Create a client with the MinIO server playground, its access key
    # and secret key.
client = Minio("play.min.io",
        access_key="5cERnGi8t6b0RFMLsaMf",
        secret_key="g9mzHGVwwPkluctM16PI1AMWTmJLnbXwxBdIJMaH",
    )
    