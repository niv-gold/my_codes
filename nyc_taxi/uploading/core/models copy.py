from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

@dataclass(frozen=True)
class FileIdentity:
    '''This class represent a file on the local computer'''
    path: Path
    file_size: int
    create_date: datetime

    @property
    def file_name(self)-> str:
        return self.path.name

    @property
    def stable_key(self)-> str:
        return f"{self.file_name}'|'{self.file_size}'|'{int(self.create_date.timestamp())}"
    
if __name__ == '__main__':
    file_instance_1 = FileIdentity(Path('niv/sub/sub2/new_file.csv'),123456, datetime.now())
    print(file_instance_1.stable_key)
    print(file_instance_1.file_name)
