from nyc_taxi.uploading.infra.fakes import FakeArchiver, FakeFileFinder, FakeUploader, FakeLoadLogRepository
from nyc_taxi.uploading.core.pipeline import IngestionPipeline
from nyc_taxi.uploading.core.models import FileIdentity
from pathlib import Path
from datetime import datetime

now = datetime.now()

def main()-> None:
    ff = FakeFileFinder.list_files()
    print(ff)
    

    file = FileIdentity(Path('file_3.parquet'),500,now)
    upl = FakeUploader.upload(file)
    print(upl)

    FakeArchiver.archive(file)

    digested_keys = ['1','2','3','4','5','6']
    alredy_uploaded_keys = FakeLoadLogRepository.already_loaded(digested_keys)
    print(alredy_uploaded_keys)

    FakeLoadLogRepository.log_success(file, 'S3//::nyc_taxi/log_index/')
    FakeLoadLogRepository.log_failure(file, 'Stack Overflow ;(')

    FakePipeline = IngestionPipeline(FileIdentity,FakeUploader,FakeLoadLogRepository,FakeArchiver)
    FakePipeline.run()
    
if __name__ == '__main__':
    main()