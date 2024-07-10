import asyncio
import inspect
import httpx
from tests.sample_data.mock_api import generate_data, get_api, ingest_endpoint
import mad_prefect.filesystems

# Override the environment variable before importing register_mad_filesystem
mad_prefect.filesystems.FILESYSTEM_URL = "file://./tests/sample_data"


## Test functions for generate_data ##


# Testing offset param greater than record_cap produces None
async def test_offset_over_cap():
    params = {"limit": 100, "offset": 400}
    data = await generate_data(params=params)
    assert data is None


# Testing record_count is restricted by record_cap
async def test_record_count():
    params = {"limit": 100, "offset": 400}
    record_cap = 410
    data = await generate_data(params=params, record_cap=record_cap)
    record_count = data["record_count"]
    assert record_count == 10


# Testing unique records are reproducible
async def test_records_reproducible():
    # retrieve first 2 records
    params_1 = {"limit": 2, "offset": 0}
    data_1 = await generate_data(params=params_1)

    # retrieve just second record
    params_2 = {"limit": 1, "offset": 1}
    data_2 = await generate_data(params=params_2)

    record_id_data_1 = data_1["organisations"][1]["organisation_id"]
    record_id_data_2 = data_2["organisations"][0]["organisation_id"]

    # compare organisation_id
    assert record_id_data_1 == record_id_data_2


## Test functions for get_api ##


# Testing object type returned when return_type set to json
async def test_json_output_type():
    params = {"limit": 2, "offset": 0}

    # Deliberately not setting return_type as this should default to "json"
    response = await get_api(params=params)
    assert isinstance(response, (list, dict))


# Testing object type returned when return_type set to "api_response" is httpx.Response
async def test_response_output_type():
    params = {"limit": 2, "offset": 0}
    return_type = "api_response"
    response = await get_api(params=params, return_type=return_type)
    assert isinstance(response, httpx.Response)


# Testing params can be extracted from httpx.Response
async def test_param_extraction():
    # Make the API Call
    params = {"limit": 50, "offset": 100}
    return_type = "api_response"
    response = await get_api(params=params, return_type=return_type)

    # Extract the params
    extracted_params = dict(response.request.url.params)
    limit = extracted_params["limit"]
    offset = extracted_params["offset"]

    # Ensure they match input
    assert limit == 50
    assert offset == 100


# Testing outcome of inspect.isasyncgen on return function
async def test_isasyncgen_on_return():
    print(inspect.isasyncgen(get_api()))


## Testing functions for ingest_endpoint ##


# Testing output type is async_generator
async def test_iterator_output():
    assert inspect.isasyncgen(ingest_endpoint())


# Testing params can be extracted from async_generator
async def test_iterator_param_extraction():
    # Set up list for checking after iteration
    limit_list = []
    offset_list = []

    # Iterate through function and append param values to list
    async for response in ingest_endpoint(return_type="api_response"):
        extracted_params = dict(response.request.url.params)
        limit_list.append(extracted_params["limit"])
        offset_list.append(extracted_params["offset"])

    # List lengths should be 3 as each limit is set to 100 and record_cap defaults to 267
    # Therefore the iteration loop should break after 3 iterations
    assert len(limit_list) == 3 and len(offset_list) == 3

    # First offset value should equal 0 str()
    assert offset_list[0] == "0"

    # Second offset value should be 100 str
    assert offset_list[1] == "100"

    # Third offset value should be 200 str
    assert offset_list[2] == "200"


if __name__ == "__main__":
    asyncio.run(test_isasyncgen_on_return())
