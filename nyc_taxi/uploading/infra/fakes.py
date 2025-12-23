from __future__ import annotations
from nyc_taxi.uploading.core.models import FileIdentity
from nyc_taxi.uploading.core.ports import Uploader, Archiver, LoadLogRepository, FileFinder
from datetime import datetime
from pathlib import Path


now = datetime.now()

class FakeFileFinder(FileFinder):
    def list_files()-> list[FileIdentity]:        
        f_list = [FileIdentity(Path('file_1.csv'),10,now),
                  FileIdentity(Path('file_2.csv'),23,now)]
        return f_list

class FakeUploader(Uploader):
    def upload(file: FileIdentity)-> str:
        return f'S3:://nyc_taxi/raw/{file.name}'

class FakeArchiver(Archiver):
    def archive(file: FileIdentity)-> None:
        print(f'file {file.name} Archived: Success!!!')

class FakeLoadLogRepository(LoadLogRepository):
    def already_loaded(file_keys: list[str]) -> set[str]:
        exists_keys = ['1','2','3']
        used_keys = [key for key in file_keys if key in exists_keys]
        return used_keys
    
    def log_success(file: FileIdentity, destination: str)-> None:
        print(f'File name: {file.name}, logged at: {destination}')

    def log_failure(file: FileIdentity, error: str)-> None:
        print(f'file name: {file.name}, FAIL logging' )
        print(f'error message: {error}')