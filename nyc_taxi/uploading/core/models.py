from __future__ import annotations
from datetime import datetime
from dataclasses import dataclass 
from pathlib import Path

@dataclass(frozen=True)
class FileIdentity:
    path: Path
    size_byte: int
    modified_time: datetime
    
    @property
    def name(self)-> str:
        return self.path.name
