import asyncio
from datetime import datetime
from faker import Faker
import random
import json
import httpx
import mad_prefect.filesystems

# Override the environment variable before importing register_mad_filesystem
mad_prefect.filesystems.FILESYSTEM_URL = "file://./tests/sample_data"


async def ingest_endpoint(
    endpoint: str = "organisations",
    params: dict | None = None,
    record_cap: int | None = None,
    return_type: str = "json",
):
    if not params:
        params = {}

    limit = 100
    offset = 0

    while True:

        # params = {"limit": limit, "offset": offset, **params}
        params["limit"] = limit
        params["offset"] = offset

        response = await get_api(endpoint, params, record_cap, return_type)

        if not response:
            print("Breaking with no response")
            break

        offset += limit

        yield response


async def get_api(
    endpoint="organisations",
    params=None,
    record_cap: int | None = None,
    return_type: str = "json",
):
    data = await generate_data(endpoint, params, record_cap)

    if data is None:
        return None

    headers = {
        "Content-Type": "application/json",
        "X-API-Version": "1.0",
        "X-Generated-At": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    if params is None:
        params = {}

    url = httpx.URL(f"https://mad-prefect-api.sample-data.com/{endpoint}").copy_with(
        params=params
    )

    request = httpx.Request("GET", url)

    response = httpx.Response(
        status_code=200,
        content=json.dumps(data).encode("utf-8"),
        headers=headers,
        request=request,
    )

    if return_type == "api_response":
        return response
    else:
        return response.json()


async def generate_data(
    endpoint="organisations",
    params=None,
    record_cap: int | None = None,
):

    # Fixed starting seed defined inside the function
    starting_seed = 12345

    if params is None:
        params = {}

    # Default to 10 if not specified
    params["limit"] = params.get("limit", 10)
    # Default offset is 0
    params["offset"] = params.get("offset", 0)

    # Enforce record cap
    if record_cap is None:
        record_cap = 267

    max_limit = record_cap - params["offset"]
    record_count = min(params["limit"], max_limit)

    if params["offset"] >= record_cap:
        return None

    data = {
        "api_version": "1.0",
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "record_count": record_count,
        "offset": params["offset"],
        endpoint: [],
    }

    for i in range(record_count):
        record_seed = starting_seed + params["offset"] + i
        record = generate_record(record_seed, endpoint)
        data[endpoint].append(record)

    return data


def generate_record(seed, endpoint):
    fake = Faker()
    Faker.seed(seed)

    return {
        f"{endpoint[:-1]}_id": fake.uuid4(),
        "users": generate_users(fake, fake.random_int(min=1, max=4)),
        "products": generate_products(fake, fake.random_int(min=1, max=5)),
        "orders": generate_orders(fake, fake.random_int(min=1, max=5)),
        "reviews": generate_reviews(fake, fake.random_int(min=1, max=4)),
    }


def generate_users(fake, count):
    return [generate_user(fake) for _ in range(count)]


def generate_products(fake, count):
    return [generate_product(fake) for _ in range(count)]


def generate_orders(fake, count):
    return [generate_order(fake) for _ in range(count)]


def generate_reviews(fake, count):
    return [generate_review(fake) for _ in range(count)]


def generate_user(fake):
    return {
        "id": fake.unique.random_number(digits=5),
        "name": fake.name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
    }


def generate_product(fake):
    return {
        "id": fake.unique.random_number(digits=6),
        "name": fake.catch_phrase(),
        "price": round(fake.random.uniform(10, 1000), 2),
        "category": fake.random_element(
            elements=("Electronics", "Clothing", "Books", "Home & Garden")
        ),
    }


def generate_order(fake):
    return {
        "id": fake.unique.random_number(digits=7),
        "user_id": fake.random_number(digits=5),
        "order_date": fake.date_this_year().isoformat(),
        "status": fake.random_element(
            elements=("Pending", "Shipped", "Delivered", "Cancelled")
        ),
    }


def generate_review(fake):
    return {
        "id": fake.unique.random_number(digits=8),
        "product_id": fake.random_number(digits=6),
        "user_id": fake.random_number(digits=5),
        "rating": fake.random_int(min=1, max=5),
        "comment": fake.text(max_nb_chars=50),
    }


if __name__ == "__main__":
    asyncio.run(generate_data("organisations", {"limit": 3}))
