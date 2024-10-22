## 1.2.0 (2024-10-22)

### Feat

- add sample_size with default value to ReadJsonOptions (#4)

## 1.1.0 (2024-10-02)

### Feat

- add support for prefect v3 (#3)

## 1.0.1 (2024-10-02)

### Fix

- **MADJSONEncoder**: encode datetime.time in isoformat (#1)

## 1.0.0 (2024-10-02)

### Feat

- add timestamp format to ReadJsonOptions
- **data_assets**: caching
- ReadJsonOptions building into duckdb query
- support duckdb 1.1
- add with_options builder pattern for dat aassets
- data artifact query with optional column data types override
- add artifact filetype and default to parquet
- yield arrow table batches
- introduce concept of data asset run
- partition snapshot artifacts by year, month, day
- disable snapshotting by default feat: when snapshotting is disabled, delete the artifact dir before running
- add recursive parameter to delete_path
- add exists to MadFileSystem fix: do not yield DataArtifacts which dont exist
- **data_asset**: assets return themselves after being called, to all for composing assets of other assets
- **data_assets**: writing metadata prior to processing function for improved transparency
- **data_assets**: Set up DataAssetArtifact and enhanced how artifact paths work.
- **data_assets**: removed query persistence dependency from get_data_by_asset_name
- **data_assets**: allow assets to be bound with arguments
- implement delete_path
- Register mad:// protocol for FsspecFileSystem
- Add mkdirs method to FsspecFileSystem

### Fix

- asset run metadata miliseconds calculation
- ensure json serializer serializes datetimes consistently otherwise duckdb will have trouble retaining the timestamp data type querying it later
- **data_assets**: DataFrame support
- add additional completed print and rstrip the path
- collector to handle scenarios where fragment is already a DataArtifact
- prevent writing of empty json artifacts
- parquet columns should not be overrided
- remove double underscores to disable name mangliung
- remove class level definitions of properties
- add check to see if mad fs is already registered
- include poetry.lock
- ensure not prefect 3.x
- remove improts
- ensure single objects (like dict) are wrapped into a Sequence
- fragment increments even when no file is produced
- revert artifact default filetype to json
- prevent empty parquet or fragment artifacts from combining into the data asset
- sanitize data for Parquet serialization (remove empty structs)
- use arrow_reader to correctly stream batches
- sanitize data for json
- close the parquet writer once complete
- try schema evolution
- create a new instance of data asset whenever it is materialized for the first time with arguments
- ensure bound_arguments is an instance variable
- try and handle schema mutation
- ensure run is bound to class instance
- use python's built in format so can reference nested properties
- allow DataAssets to be passed in params
- check if path exists before deleting
- def should have been async
- do not persist if no data
- handling for when the data asset yields not artifacts
- only merge globs if there are any
- isoformat dates when serializing to json
- enumerate all descendants properly
- convert UUID types recursively
- convert UUID types to strings for pyarrow
- pass duckdb query to data asset result artifact, so the data artifact can handle creating itself
- use prepared bound args and kwargs for with_arguments
- handle non async generators and non async materialization fns
- Adding pyarrow to remove need to add this to all downstream projects for write operations
- **data_assets**: Fix issues with fragment counting glob processing
- **data_asset**: accounted for scenarios where there are no metadata or no artifact_globs
- strip ending / character
- deprecation warning
- check if generator without calling the __fn
- only give path args so they can be fixed, all other args proxied via kwargs
- implement _rm method
- allow duckdb to be 0.9 to 0.10.2

### Refactor

- BREAKING: artifact_columns into a need class ReadJsonOptions
- encapsulate artifact collector
- move file management to the individual _persist_[filetypes]
- encapsulate meta data saving in run, track materialized date and duration miliseconds
- resolve paths, name and artifacts dir after with_arguments
- data artifact represent a single atomic file and handles querying and persisting the data to the file, while data asset is responsible for materializing a group of data artifacts together into a new merged data artifact result
- encapsulate in separate files
- **data_asset**: Added name to fixture 11 test for clarity
- move DataAsset to its own file
- encapsulate data assets in own folder
- handle generator functions in DataAsset class
