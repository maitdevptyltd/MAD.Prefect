## Typical Pipeline Plan ##


Typical API Functions
fetch_token - where required this function will retrieve auth token using env variables
apply_auth - applies environment variables including the auth to the async client
get_api_client - sets up the client and uses apply_auth to connect necessary auth and env variables
build_path - builds an endpoint the file path write operations based on API call parameters.
get_api -  uses client to make api get request. Converts response to json, uses build_path to file destination and typically writes to the raw zone. 
            Either returns json data or httpx.Response object. 
ingest_endpoint - handles paginated or offset/limit APIs, iterating through necessary params and calling get_api. 
            This function would yield each API response, either in json or httpx.Response  


Asset Setup Functions
Either calls ingest_enpoint for a specific endpoint and returns an iterator through reusing yield;
Or calls get_api and uses return statement;
Or queries an existing asset and performs a transformation with return statement

DataAsset Class/Decorator
DataAsset materialization function can support yield and return
    - objects yielded are stored as artifact fragments, yield can be used to handle paginated assets
    - returned objects are stored directly into path, filetype indicated by the path file extension
DataAsset can support many yield/return types
    - regular python arrays/dicts
    - raw text/string
DataAsset can support a custom/provided artifacts directory
    - @asset(path="..", artifact_path="raw/..")
    - when ommitted, artifact_path defaults to path + "_artifacts"

