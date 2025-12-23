from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Iterable
from .models import FileIdentity

class FileFinder(ABC):
    @abstractmethod
    def list_files(self) -> list[FileIdentity]:
        pass

class Uploader(ABC):
    @abstractmethod
    def upload(self, file: FileIdentity) -> str:
        """Return destination reference (e.g., s3://bucket/key)."""
        pass

class LoadLogRepository(ABC):
    @abstractmethod
    def already_loaded(self, file_keys: list[str]) -> set[str]:
        """Return subset of file_keys already loaded successfully."""
        pass

    @abstractmethod
    def log_success(self, file: FileIdentity, destination: str) -> None:
        pass

    @abstractmethod
    def log_failure(self, file: FileIdentity, error: str) -> None:
        pass

class Archiver(ABC):
    @abstractmethod
    def archive(self, file: FileIdentity) -> None:
        pass