# upload/s3_client.py
import boto3
from nyc_taxi.uploading.config.config import S3Config

class S3Client:
    def __init__(self, config: S3Config):
        self._config = config
        self._client = boto3.client(
            "s3",
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            region_name=config.region_name,
        )
    
    def extract_and_load_file(self, local_file_path: str, s3_key: str):
        """Uploads a file to the specified S3 bucket and key."""
        self._client.upload_file(
            Filename=local_file_path,
            Bucket=self._config.bucket_name,
            Key=self._config.s3_base_prefix + s3_key
        )
        print(f"Uploaded {local_file_path} to s3://{self._config.bucket_name}/{s3_key}")


if __name__ == "__main__":
    config = S3Config()
    s3_client = S3Client(config)
    s3_client.upload_file(
        local_file_path='/home/niv/home/GitHubeRepos/my_codes/nyc_taxi/upload/data files/taxi_zone_lookup.csv',
        s3_key='raw/taxi_zone_lookup.csv'
    )