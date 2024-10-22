from pydantic import BaseModel
from typing import Dict, Optional


class ReadJsonOptions(BaseModel):
    hive_partitioning: Optional[bool] = True
    union_by_name: Optional[bool] = True
    maximum_object_size: Optional[int] = 33554432
    format: Optional[str] = "auto"

    # By default, always try and parse a nested object as a struct
    field_appearance_threshold: Optional[int] = 0
    map_inference_threshold: Optional[int] = -1
    sample_size: Optional[int] = -1

    columns: Optional[Dict[str, str]] = None
    timestampformat: Optional[str] = None
    auto_detect: Optional[bool] = None
    compression: Optional[str] = None
    convert_strings_to_integers: Optional[bool] = None
    dateformat: Optional[str] = None
    filename: Optional[bool] = None
    ignore_errors: Optional[bool] = None
    maximum_depth: Optional[int] = None
    records: Optional[str] = None
