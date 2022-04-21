import os
from copy import deepcopy
from enum import Enum, auto
from typing import Union, IO
from mojap_metadata import Metadata
import re


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

        if "PARQUET" in s:
            return cls["PARQUET"]
        elif "JSON" in s:
            return cls["JSON"]
        elif "CSV" in s:
            return cls["CSV"]
        else:
            raise ValueError(f"Cannot infer type from given string: {string}")


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


def validate_and_enrich_metadata(metadata: Union[Metadata, dict]) -> Metadata:
    m = Metadata.from_infer(metadata)
    m = deepcopy(m)
    m.set_col_type_category_from_types()
    return m


def human_to_bytes(memory: str) -> int:
    """Convert a human-readable representation of memory to bytes.
    Argument:
    memory: str - a human-readable amount of memory e.g. '20 GB' or '5MB'
    Returns:
    the number of bytes in memory
    """

    mult = {"k": 10**3, "m": 10**6, "g": 10**9, "t": 10**12}

    m = re.match(r"(\d+(.\d+)?)\s*([kKmMgGtT]?B)", memory)
    if m:
        x = float(m.group(1))
        m = mult[m.group(3)[0].lower()]
        return int(x * m)

