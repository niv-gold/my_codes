# ========================================================
# S3 File Upload Configuration Management
# ========================================================
# Purpose: Centralized configuration dataclass for managing
#          AWS S3 upload credentials and file paths
#
# This module defines the S3config dataclass which serves as
# a single source of truth for all S3 upload settings:
# - AWS authentication credentials (access key, secret key)
# - S3 bucket and folder location
# - Local file paths for batch processing
# - Optional S3 prefix for organizing uploaded files
#
# Goal: Provide one place to hold all settings (bucket name,
#        local path, credentials, etc.) for easy management
#        and reuse across the upload pipeline.
# ========================================================

from dataclasses import dataclass
import os
from dotenv import load_dotenv

# Load environment variables from .env into os.environ (silent if no file)
load_dotenv()


@dataclass
class S3Config:
    """
    Configuration dataclass for AWS S3 file uploads.
    
    This dataclass encapsulates all parameters needed to connect
    to AWS S3 and upload files from local storage.
    
    Attributes:
        aws_access_key_id (str): AWS access key for authentication
        aws_secret_access_key (str): AWS secret key for authentication
        region_name (str): AWS region where the S3 bucket is located
                          (e.g., 'us-east-1', 'eu-west-1')
        bucket_name (str): Name of the S3 bucket to upload files to
        local_base_path (str): Local directory path where files to upload
                              are located (e.g., '/data/uploads/')
        s3_base_prefix (str, optional): Prefix/folder structure in S3 bucket
                                       (e.g., 'raw/', 'staging/')
                                       Defaults to '' (bucket root)
    
    Example:
        >>> config = S3Config(
        ...     aws_access_key_id='YOUR_KEY_ID',
        ...     aws_secret_access_key='YOUR_SECRET',
        ...     region_name='us-east-1',
        ...     bucket_name='my-bucket',
        ...     local_base_path='/home/user/data/',
        ...     s3_base_prefix='raw/'
        ... )
        >>> # This creates S3 path: s3://my-bucket/raw/
    """
    # Read values from environment (defaults to empty string when missing)
    aws_access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID", None)
    aws_secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", None)
    region_name: str = os.getenv("AWS_REGION", None)
    bucket_name: str = os.getenv("S3_BUCKET_NAME", None)
    local_base_path: str = os.getenv("LOCAL_BASE_PATH", None)
    s3_base_prefix: str = os.getenv("S3_BASE_PREFIX", None)
    local_file_extension: str = os.getenv("LOCAL_FILE_EXTENSION", None)


###############
## Unit Test ##
###############
# print .env variables

if __name__ == "__main__":
    # Example instantiation for testing purposes
    config = S3Config()
    print('--> Print 1 ','~'*150)
    print(config) 
    print('--> Print 2 ','~'*150)
    print("AWS Access Key ID:", config.aws_access_key_id)
    print("AWS Secret Access Key:", config.aws_secret_access_key)
    print("AWS Region Name:", config.region_name)
    print("S3 Bucket Name:", config.bucket_name)
    print("Local Base Path:", config.local_base_path)
    print("S3 Base Prefix:", config.s3_base_prefix)
    print("Local File Extension:", config.local_file_extension)
