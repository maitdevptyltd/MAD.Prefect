import asyncio
from faker import Faker
import random
import json
import mad_prefect.filesystems

# Override the environment variable before importing register_mad_filesystem
mad_prefect.filesystems.FILESYSTEM_URL = "file://./tests/synthetic_data"


async def get_api(endpoint="organisations", params=None):
    fs = await mad_prefect.filesystems.get_fs()

    # Fixed starting seed defined inside the function
    starting_seed = 12345

    if params is None:
        params = {}

    # Default to 5 if not specified
    limit = params.get("limit", 5)
    # Default offset is 0
    offset = params.get("offset", 0)

    # Apply offset to the starting seed
    adjusted_seed = starting_seed + offset

    fake = Faker()
    random.seed(adjusted_seed)
    Faker.seed(adjusted_seed)

    data = {
        "api_version": "1.0",
        "timestamp": fake.iso8601(),
        "record_count": limit,
        "offset": offset,
        endpoint: [],
    }

    for i in range(limit):
        record_seed = random.randint(1, 100000)
        record = generate_record(fake, record_seed, endpoint)
        data[endpoint].append(record)

    await fs.write_data(f"raw/{endpoint}.json", data)
    return data


def generate_record(fake, seed, endpoint):
    random.seed(seed)
    Faker.seed(seed)

    return {
        f"{endpoint[:-1]}_id": fake.uuid4(),
        "users": generate_users(fake, random.randint(1, 4)),
        "products": generate_products(fake, random.randint(1, 5)),
        "orders": generate_orders(fake, random.randint(1, 5)),
        "reviews": generate_reviews(fake, random.randint(1, 4)),
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
    asyncio.run(get_api("organisations", {"limit": 2}))
