# MAD Prefect Data Assets - Developer Documentation

## Introduction

The `mad_prefect` library introduces a powerful pattern for managing data within Prefect workflows using **data assets**. A data asset represents a unit of data that can be materialized, cached, and queried independently. By leveraging data assets, you can build modular, reusable, and maintainable data pipelines that are efficient and easy to reason about.

At the core of this pattern is the `@asset` decorator, which transforms a regular Python function into a data asset. This decorator handles the intricacies of data persistence, caching, artifact management, and provides querying capabilities using DuckDB.

**Example:**

```python
from datetime import timedelta
from mad_prefect.data_assets import asset

@asset(
    path="/data/results.json",
    name="my_data_asset",
    artifact_filetype="json",
    cache_expiration=timedelta(hours=1),
)
def generate_data():
    # Data generation logic
    data = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
    return data

# Executing the data asset
result_artifact = await generate_data()

# Querying the data asset
query_result = await generate_data.query("WHERE id > 1")
```

In this example, `generate_data` is defined as a data asset using the `@asset` decorator. When executed, it automatically handles data persistence to the specified path, caching based on the `cache_expiration`, and allows querying the data without loading it entirely into memory.

**Power of the Data Asset Pattern:**

- **Modularity and Reusability:** Encapsulate data logic in self-contained units that can be reused across different workflows.
- **Automatic Caching:** Avoid redundant computations by caching results, leading to performance improvements.
- **Efficient Data Handling:** Process large datasets efficiently by handling data in batches and using disk-based storage.
- **Seamless Querying:** Utilize DuckDB's powerful SQL capabilities to query data assets directly.
- **Historical Data Management:** Use features like `snapshot_artifacts` to maintain historical versions of data for auditing and rollback.

**Use Cases:**

- **Data Transformation Pipelines:** Build ETL processes where each step is a data asset that can be independently executed and queried.
- **Machine Learning Workflows:** Prepare datasets, cache intermediate results, and efficiently query data for model training and evaluation.
- **Data Analysis and Reporting:** Enable data analysts to access preprocessed data assets for reporting without needing to understand the underlying data retrieval mechanisms.
- **Incremental Data Processing:** Handle data that arrives incrementally by processing new data and updating the data assets accordingly.

---

## Table of Contents

1. [Modules](#modules)
   - [Asset Decorator](#asset-decorator)
   - [DataAsset Class](#dataasset-class)
   - [DataArtifact Class](#dataartifact-class)
   - [DataArtifactCollector Class](#dataartifactcollector-class)
   - [DataArtifactQuery Class](#dataartifactquery-class)
   - [DataAssetRun Class](#dataassetrun-class)
   - [Utilities](#utilities)
     - [yield_data_batches Function](#yield_data_batches-function)
     - [register_mad_protocol Function](#register_mad_protocol-function)
     - [get_fs Function](#get_fs-function)
   - [Filesystems](#filesystems)
     - [FsspecFileSystem Class](#fsspecfilesystem-class)
     - [MadFileSystem Class](#madfilesystem-class)

2. [Usage Examples](#usage-examples)
3. [Notes](#notes)
4. [Best Practices](#best-practices)
5. [Troubleshooting](#troubleshooting)
6. [Extending Functionality](#extending-functionality)
7. [Contributing](#contributing)

---

## Modules

### Asset Decorator

#### `asset`

```python
def asset(
    path: str,
    artifacts_dir: str = "",
    name: str | None = None,
    snapshot_artifacts: bool = False,
    artifact_filetype: ARTIFACT_FILE_TYPES = "json",
    read_json_options: ReadJsonOptions | None = None,
    read_csv_options: ReadCSVOptions | None = None,
    cache_expiration: datetime.timedelta | None = None,
):
    ...
```

The `asset` decorator is used to define a data asset within a Prefect flow. It wraps a function and returns a `DataAsset` instance that manages the data asset lifecycle, including caching, persistence, and querying.

**Parameters:**

- `path` (str): The path where the final result artifact will be stored.
    - Supports multiple file types using a |-delimited syntax 
    - e.g. "path/to/file.parquet|csv" will produce two result artifacts at "path/to/file.parquet" and "path/to/file.csv"
- `artifacts_dir` (str, optional): The directory where intermediate artifacts will be stored.
- `name` (str, optional): The name of the data asset. If not provided, defaults to the function name.
- `snapshot_artifacts` (bool, optional): Whether to snapshot artifacts over time.
- `artifact_filetype` (Literal["parquet", "json", "csv"], optional): The file type for intermediate artifacts.
- `read_json_options` (ReadJsonOptions, optional): Options for reading JSON data.
- `read_csv_options` (ReadCSVOptions, optional): Options for reading comma separated values data.
- `cache_expiration` (datetime.timedelta, optional): The cache expiration time. If data has been materialized within this period, it will be reused.

**Usage:**

```python
@asset(path="/data/results.json", name="my_asset")
def generate_data():
    # Generate data logic
    return data
```

---

### DataAsset Class

#### `DataAsset`

```python
class DataAsset:
    def __init__(
        self,
        fn: Callable,
        path: str,
        artifacts_dir: str = "",
        name: str | None = None,
        snapshot_artifacts: bool = False,
        artifact_filetype: ARTIFACT_FILE_TYPES = "json",
        read_json_options: ReadJsonOptions | None = None,
        read_csv_options: ReadCSVOptions | None = None,
        cache_expiration: timedelta | None = None,
    ):
        ...
```

The `DataAsset` class represents a data asset in the system. It manages the execution of the associated function, caching, artifact management, and querying capabilities.

**Key Methods:**

- `with_arguments(*args, **kwargs)`: Returns a new `DataAsset` instance with the provided arguments bound.
- `with_options(...)`: Returns a new `DataAsset` instance with overridden options.
- `__call__(self, *args, **kwargs)`: Executes the data asset, handling caching and persistence.
- `query(self, query_str: str | None = None)`: Queries the data asset using DuckDB.

**Properties:**

- `name`: The name of the data asset.
- `path`: The path where the final result artifact is stored.
- `artifact_filetype`: The file type for artifacts (e.g., "json", "parquet", "csv").

---

### DataArtifact Class

#### `DataArtifact`

```python
class DataArtifact:
    def __init__(
        self,
        path: str,
        data: object | None = None,
        read_json_options: ReadJsonOptions | None = None,
        read_csv_options: ReadCSVOptions | None = None,
    ):
        ...
```

The `DataArtifact` class represents an individual data artifact, which can be a fragment of data or the final result. It handles persistence and querying of the data.

**Key Methods:**

- `persist(self)`: Persists the data artifact to the filesystem.
- `query(self, query_str: str | None = None)`: Queries the artifact data using DuckDB.
- `exists(self)`: Checks if the artifact exists on the filesystem.

**Usage:**

Data artifacts are usually managed internally by `DataAsset` and `DataArtifactCollector`, but can be interacted with directly if needed.

---

### DataArtifactCollector Class

#### `DataArtifactCollector`

```python
class DataArtifactCollector:
    def __init__(
        self,
        collector: object,
        dir: str,
        filetype: ARTIFACT_FILE_TYPES = "json",
        artifacts: list[DataArtifact] | None = None,
        read_json_options: ReadJsonOptions | None = None,
        read_csv_options: ReadCSVOptions | None = None,
    ):
        ...
```

The `DataArtifactCollector` class is responsible for collecting data artifacts from a data generation function, persisting them, and tracking their locations.

**Key Methods:**

- `collect(self)`: Asynchronously collects data artifacts by persisting each batch of data.

---

### DataArtifactQuery Class

#### `DataArtifactQuery`

```python
class DataArtifactQuery:
    def __init__(
        self,
        artifacts: list[DataArtifact] | None = None,
        read_json_options: ReadJsonOptions | None = None,
        read_csv_options: ReadCSVOptions | None = None,
    ):
        ...
```

The `DataArtifactQuery` class provides functionality to query multiple data artifacts using DuckDB.

**Key Methods:**

- `query(self, query_str: str | None = None)`: Executes a query against the combined data of the provided artifacts.

---

### DataAssetRun Class

#### `DataAssetRun`

```python
class DataAssetRun(BaseModel):
    id: str | None = None
    runtime: datetime.datetime | None = None
    materialized: datetime.datetime | None = None
    duration_miliseconds: int | None = None
    asset_id: str | None = None
    asset_name: str | None = None
    asset_path: str | None = None
    parameters: str | None = None

    async def persist(self):
        ...
```

The `DataAssetRun` class represents a single execution (run) of a data asset. It tracks metadata such as runtime, duration, and parameters used.

---

### Utilities

#### `yield_data_batches` Function

```python
async def yield_data_batches(data: object):
    ...
```

The `yield_data_batches` function is a utility that yields data batches from various types of data sources, such as coroutines, generators, and async generators.

**Usage:**

Used internally by `DataArtifact` and `DataArtifactCollector` to handle different types of data sources uniformly.

---

#### `register_mad_protocol` Function

```python
async def register_mad_protocol(connection: duckdb.DuckDBPyConnection | None = None):
    ...
```

Registers the custom "mad" filesystem protocol with DuckDB, allowing DuckDB to read data from the custom filesystem used by `mad_prefect`.

**Usage:**

Called before executing queries that involve the "mad://" protocol.

---

#### `get_fs` Function

```python
async def get_fs():
    ...
```

Returns an instance of `FsspecFileSystem` configured with the appropriate filesystem URL and options.

**Usage:**

Used internally whenever filesystem access is required.

---

### Filesystems

#### `FsspecFileSystem` Class

```python
class FsspecFileSystem(
    prefect.filesystems.WritableFileSystem,
    prefect.filesystems.WritableDeploymentStorage,
):
    ...
```

`FsspecFileSystem` is a custom filesystem class that extends Prefect's `WritableFileSystem` and `WritableDeploymentStorage`. It uses `fsspec` to interact with various filesystems.

**Key Methods:**

- `write_path(self, path: str, content: bytes)`: Writes data to the specified path.
- `read_path(self, path: str)`: Reads data from the specified path.
- `exists(self, path: str)`: Checks if the path exists.
- `delete_path(self, path: str, recursive: bool = False)`: Deletes the specified path.

**Configuration:**

- `basepath`: The base path for the filesystem.
- `storage_options`: Options for configuring the underlying filesystem (e.g., authentication credentials).

---

#### `MadFileSystem` Class

```python
class MadFileSystem(DirFileSystem):
    ...
```

`MadFileSystem` is a custom filesystem class that extends `fsspec`'s `DirFileSystem`. It is used to integrate with DuckDB by providing a filesystem interface that DuckDB can use.

---

## Usage Examples

### Defining a Data Asset

```python
from mad_prefect.data_assets import asset

@asset(path="/data/results.json", name="my_data_asset")
def generate_data():
    # Your data generation logic here
    data = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
    return data
```

### Executing a Data Asset

```python
# Execute the data asset
result_artifact = await generate_data()

# The result_artifact is a DataArtifact instance
```

### Querying a Data Asset

```python
# Query the data asset
query_result = await generate_data.query("WHERE id > 1")
```

### Using `with_arguments`

```python
@asset(path="/data/{dataset_name}.json", name="dataset_{dataset_name}")
def generate_dataset(dataset_name: str):
    # Generate data based on dataset_name
    data = fetch_data(dataset_name)
    return data

# Execute with specific arguments
dataset_asset = generate_dataset.with_arguments(dataset_name="users")
result_artifact = await dataset_asset()
```

### Configuring Filesystem

Set environment variables to configure the filesystem:

```bash
export FILESYSTEM_URL="s3://my-bucket"
export FILESYSTEM_BLOCK_NAME="my_s3_block"
```

---

## Notes

- **Caching:** Data assets support caching based on the `cache_expiration` parameter. If data has been materialized within the expiration period, the cached result will be used.
- **Artifacts:** Intermediate artifacts are stored in the `artifacts_dir`. If `snapshot_artifacts` is enabled, artifacts are stored with timestamps to allow historical data inspection.
- **File Types:** Supports "json" and "parquet" file types for artifacts. Ensure consistency when querying multiple artifacts.
- **Filesystem Integration:** Uses `fsspec` for filesystem abstraction, allowing interaction with various storage systems (local, S3, etc.).

---

## Best Practices

- **Consistent File Types:** When collecting artifacts, ensure they are all of the same file type to avoid querying issues.
- **Error Handling:** Be mindful of the data types returned by your data generation functions to ensure they are compatible with the persistence mechanisms.
- **Filesystem Configuration:** Properly configure your filesystem via environment variables or block storage to ensure data is read from and written to the correct locations.
- **Parameterization:** Use the `with_arguments` method to create parameterized data assets for different datasets or configurations.

---

## Troubleshooting

- **Data Not Persisted:** Check if the data returned is empty or falsy, which could prevent the artifact from being persisted.
- **DuckDB Errors:** Ensure the "mad" protocol is registered before executing queries involving `mad://` URIs.
- **Filesystem Access Issues:** Verify that the filesystem is correctly configured and that necessary credentials are provided.
- **Schema Mismatches:** When querying artifacts, ensure that all artifacts have compatible schemas, especially when using different data sources.

---

## Extending Functionality

Developers can extend the functionality of `mad_prefect` by:

- **Adding Support for Additional File Types:** Extend `DataArtifact` and `DataArtifactQuery` to handle more file formats (e.g., Avro).
- **Custom Persistence Strategies:** Implement new methods in `DataArtifact` for persisting data using different storage mechanisms.
- **Enhanced Filesystem Features:** Extend `FsspecFileSystem` with additional methods or support for more complex storage options.
- **Integrate with Other Databases:** Adapt the querying capabilities to work with databases other than DuckDB if needed.

---

## Contributing

Contributions to `mad_prefect` are welcome. Please follow the project's contribution guidelines and ensure that new features are accompanied by tests and documentation.

- **Code Style:** Adhere to PEP 8 guidelines and use type hints where appropriate.
- **Testing:** Write unit tests for new features or bug fixes.
- **Documentation:** Update the documentation to reflect changes and additions.
- **Issue Reporting:** Use the issue tracker to report bugs or suggest enhancements.

---