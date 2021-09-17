import os
from enum import Enum, auto
from typing import Union, IO
from mojap_metadata import Metadata


class FileFormatNotFound(Exception):
    pass


class EngineNotImplementedError(Exception):
    pass


class FileFormat(Enum):
    PARQUET = auto()
    JSON = auto()
    CSV = auto()

    @classmethod
    def from_string(cls, string: str):
        s = string.strip().upper()

        if s in ["JSON", "JSONL", "NDJSON"]:
            s = "JSON"

        return cls[s]


def is_s3_filepath(input_file: Union[IO, str]) -> bool:
    if isinstance(input_file, str):
        return input_file.startswith("s3://")
    else:
        return False


def match_file_format_to_str(s: str, raise_error=False) -> Union[FileFormat, None]:
    for file_format in FileFormat.__members__.keys():
        if file_format in s.upper():
            return FileFormat[file_format]
    if raise_error:
        raise FileFormatNotFound(f"Could not determine file format from {s}")
    else:
        return None


def infer_format_from_filepath(input_file) -> FileFormat:
    fn = os.path.basename(input_file)
    _, ext = fn.split(".", 1)
    file_format = match_file_format_to_str(ext)
    if file_format:
        return file_format
    else:
        raise FileFormatNotFound(f"Could not infer file format from: {input_file}")


def infer_file_format_from_meta(metadata: Union[Metadata, dict]):
    if isinstance(metadata, Metadata):
        file_format_str = metadata.file_format
    else:
        file_format_str = metadata.get("file_format", "")
    file_format = match_file_format_to_str(file_format_str)
    if file_format:
        return file_format
    else:
        raise FileFormatNotFound("Could not infer file format from metadata")


def infer_file_format(input_file, metadata: Union[Metadata, dict] = None):
    file_format = None
    try:
        file_format = infer_format_from_filepath(input_file)
    except FileFormatNotFound:
        if metadata:
            try:
                file_format = infer_file_format_from_meta(metadata)
            except FileFormatNotFound:
                pass

    if file_format:
        return file_format
    else:
        raise FileFormatNotFound(
            "Could not infer file_format from input_file or metadata"
        )
