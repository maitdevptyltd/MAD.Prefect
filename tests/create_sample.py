import asyncio
import duckdb
from mad_prefect.filesystems import get_fs
import mad_prefect.filesystems
from mad_prefect.duckdb import register_mad_protocol


mad_prefect.filesystems.FILESYSTEM_URL = "file://./tests"


async def create_sample_data():
    data = [
        {
            "id": 1,
            "name": "Alice",
            "age": 30,
            "address": {
                "city": "New York",
                "details": {"street": "5th Avenue", "zipcode": "10001"},
            },
            "employment": {"status": "employed", "position": "Engineer"},
            "hobbies": ["reading", "traveling", "swimming"],
            "contact_details": {
                "mobile_number": 61465382449,
                "home_phone": "0732766068",
                "email": "alice_smith@gmail.com",
            },
        },
        {
            "id": 2,
            "name": "Bob",
            "age": 25,
            "address": {
                "city": "Los Angeles",
                "details": {"street": "Sunset Blvd", "zipcode": "90001"},
            },
            "employment": {"status": "unemployed", "position": "N/A"},
            "hobbies": ["gaming", "hiking", "photography"],
            "contact_details": {
                "email": "bob_smith@gmail.com",
                "work_phone": 61732766068.0,
                "mobile_number": "0465662449",
            },
        },
        {
            "id": 3,
            "name": "Charlie",
            "age": 35,
            "address": {
                "city": "Chicago",
                "details": {"street": "Michigan Ave", "zipcode": "60601"},
            },
            "employment": {"status": "employed", "position": "Manager"},
            "hobbies": ["cooking", "cycling", "gardening"],
            "contact_details": {
                "work_phone": "0632766068",
                "email": "charlie_smith@gmail.com",
                "mobile_number": 61475382449,
            },
        },
        {
            "id": 4,
            "name": "Diana",
            "age": 28,
            "address": {
                "city": "Houston",
                "details": {"street": "Main St", "zipcode": "77002"},
            },
            "employment": {"status": "self-employed", "position": "Freelancer"},
            "hobbies": ["yoga", "painting", "dancing"],
            "contact_details": {
                "mobile_number": 61485382449,
                "email": "diana_smith@gmail.com",
                "home_phone": "0732767068",
            },
        },
        {
            "id": 5,
            "name": "Eve",
            "age": 22,
            "address": {
                "city": "San Francisco",
                "details": {"street": "Market St", "zipcode": "94103"},
            },
            "employment": {"status": "student", "position": "Intern"},
            "hobbies": ["running", "writing", "volunteering"],
            "contact_details": {
                "email": "eve_smith@gmail.com",
                "home_phone": 61732766070.0,
                "mobile_number": "0465662450",
            },
        },
        # Record with nested contact details as array of dicts
        {
            "id": 6,
            "name": "Frank",
            "age": 40,
            "address": {
                "city": "Seattle",
                "details": {"street": "Pine St", "zipcode": "98101"},
            },
            "employment": {"status": "retired"},
            "hobbies": ["fishing", "reading"],
            "contact_details": [
                {
                    "mobile_number": 61485382449,
                    "home_phone": "0732767068",
                    "email": "frank_smith@gmail.com",
                },
                {
                    "work_phone": 61732766071.0,
                    "work_email": "frank_smith_work@gmail.com",
                },
            ],
        },
        {
            "id": 7,
            "name": "Grace",
            "age": 32,
            "address": {"city": "Miami"},
            "employment": {"status": "employed", "position": "Designer"},
            "hobbies": ["surfing", "dancing"],
            "contact_details": {
                "email": "grace_smith@gmail.com",
                "work_phone": "0732766072",
                "mobile_number": 61495382450.0,
            },
        },
        {
            "id": 8,
            "name": "Hank",
            "age": 29,
            "address": {"city": "Austin"},
            "employment": {"status": "freelancer", "position": "Developer"},
            "hobbies": ["gaming", "music"],
            "contact_details": {
                "email": "hank_smith@gmail.com",
                "work_phone": "0732766073",
                "mobile_number": 61505382451,
            },
        },
        # Record with missing 'employment' entirely
        {
            "id": 9,
            "name": "Ivy",
            "age": 24,
            "address": {"city": "Austin"},
            "hobbies": ["biking", "photography"],
            "contact_details": {
                "mobile_number": 0465662452.0,
                "home_phone": 61732766074.0,
                "email": "ivy_smith@gmail.com",
            },
        },
        # Additional records with only work_phone in contact_details
        {
            "id": 10,
            "name": "Jake",
            "age": 45,
            "address": {
                "city": "Dallas",
                "details": {"street": "Elm St", "zipcode": "75201"},
            },
            "employment": {"status": "employed", "position": "Analyst"},
            "hobbies": ["reading", "traveling"],
            "contact_details": {
                "mobile_number": 61465382450,
                "home_phone": "0732766075",
                "email": "jake_smith@gmail.com",
            },
        },
        # Record with nested contact details as array of dicts
        {
            "id": 11,
            "name": "Laura",
            "age": 27,
            "address": {
                "city": "Boston",
                "details": {"street": "Boylston St", "zipcode": "02116"},
            },
            "employment": {"status": "self-employed", "position": "Consultant"},
            "hobbies": ["yoga", "painting"],
            "contact_details": [
                {
                    "mobile_number": 61465382451,
                    "home_phone": "0732766076",
                    "email": "laura_smith@gmail.com",
                },
                {
                    "work_phone": 61732766076.0,
                    "work_email": "laura_smith_work@gmail.com",
                },
            ],
        },
        {
            "id": 12,
            "name": "Michael",
            "age": 38,
            "address": {
                "city": "Philadelphia",
                "details": {"street": "Market St", "zipcode": "19103"},
            },
            "employment": {"status": "freelancer", "position": "Writer"},
            "hobbies": ["running", "writing"],
            "contact_details": {
                "mobile_number": 61465382452,
                "home_phone": "0732766077",
                "email": "michael_smith@gmail.com",
            },
        },
    ]

    await register_mad_protocol()

    fs = await get_fs()
    await fs.write_data("nested_structures/sample_json.json", data)
    query = duckdb.query(
        "SELECT id, name, age, address, employment, hobbies, CAST(contact_details AS JSON) AS contact_details FROM read_json_auto('mad://nested_structures/**/*.json')"
    )
    query.to_parquet("mad://nested_structures/sample3.parquet")


if __name__ == "__main__":
    asyncio.run(create_sample_data())