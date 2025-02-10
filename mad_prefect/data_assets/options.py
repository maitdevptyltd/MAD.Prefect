from pydantic import BaseModel
from typing import Dict, List, Optional, Union


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


class ReadCSVOptions(BaseModel):
    hive_partitioning: Optional[bool] = True
    union_by_name: Optional[bool] = True
    header: Optional[bool] = True
    auto_detect: Optional[bool] = True

    # Below are all other DuckDB read_csv parameters,
    # defaulting to None so that DuckDB's built-in defaults apply.
    all_varchar: Optional[bool] = None
    allow_quoted_nulls: Optional[bool] = None
    auto_type_candidates: Optional[List[str]] = None
    buffer_size: Optional[int] = None
    columns: Optional[Dict[str, str]] = None
    comment: Optional[str] = None
    compression: Optional[str] = None
    dateformat: Optional[str] = None
    decimal_separator: Optional[str] = None
    delim: Optional[str] = None
    escape: Optional[str] = None
    encoding: Optional[str] = None
    filename: Optional[bool] = None
    force_not_null: Optional[List[str]] = None
    ignore_errors: Optional[bool] = None
    max_line_size: Optional[int] = None
    column_names: Optional[List[str]] = None
    new_line: Optional[str] = None
    normalize_names: Optional[bool] = None
    null_padding: Optional[bool] = None
    nullstr: Optional[Union[str, List[str]]] = None
    null: Optional[Union[str, List[str]]] = None
    parallel: Optional[bool] = None
    quote: Optional[str] = None
    rejects_scan: Optional[str] = None
    rejects_table: Optional[str] = None
    rejects_limit: Optional[int] = None
    rfc_4180: Optional[bool] = None
    sample_size: Optional[int] = None
    sep: Optional[str] = None
    skip: Optional[int] = None
    store_rejects: Optional[bool] = None
    timestampformat: Optional[str] = None
    timestamp_format: Optional[str] = None
    types: Optional[Union[List[str], Dict[str, str]]] = None
    dtypes: Optional[Union[List[str], Dict[str, str]]] = None
    column_types: Optional[Union[List[str], Dict[str, str]]] = None
