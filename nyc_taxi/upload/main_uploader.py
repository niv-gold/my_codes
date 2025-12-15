from s3_client import S3Client
from config import S3Config

def run():
    s3_config = S3Config()
    s3_inst = S3Client(s3_config)
    s3_inst.extract_and_load_file(
        local_file_path='/home/niv/home/GitHubeRepos/my_codes/nyc_taxi/upload/data files/taxi_zone_lookup.csv',
        s3_key='taxi_zone_lookup123.csv'
    )

if __name__ == "__main__":
    run()