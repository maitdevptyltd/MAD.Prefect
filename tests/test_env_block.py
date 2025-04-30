import os
from typing import Optional, Union
import pytest
from pydantic import SecretStr
from mad_prefect.envblock import EnvBlock


class DownstreamAPI(EnvBlock):
    token: str
    url: str
    retries: int = 3
    secret: SecretStr
    _internal: str = "should not load"


def reset_env_and_instance(cls):
    keys = [
        "DOWNSTREAMAPI_TOKEN",
        "DOWNSTREAMAPI_URL",
        "DOWNSTREAMAPI_SECRET",
        "DOWNSTREAMAPI_RETRIES",
        "DOWNSTREAMAPI_CREDENTIAL_BLOCK_NAME",
        "DOWNSTREAMAPI__INTERNAL",
    ]
    for key in keys:
        os.environ.pop(key, None)
    cls._instance = None


async def test_loads_env_values():
    DownstreamAPI._instance = None
    test_env = {
        "DOWNSTREAMAPI_TOKEN": "abc",
        "DOWNSTREAMAPI_URL": "https://x.com",
        "DOWNSTREAMAPI_SECRET": "shhh",
    }

    creds = await DownstreamAPI.from_env(test_env)
    assert creds.token == "abc"
    assert creds.url == "https://x.com"
    assert isinstance(creds.secret, SecretStr)
    assert creds.secret.get_secret_value() == "shhh"


async def test_uses_default_value():
    DownstreamAPI._instance = None
    test_env = {
        "DOWNSTREAMAPI_TOKEN": "abc",
        "DOWNSTREAMAPI_URL": "https://x.com",
        "DOWNSTREAMAPI_SECRET": "shhh",
    }

    creds = await DownstreamAPI.from_env(test_env)
    assert creds.retries == 3


async def test_missing_required_field_raises():
    DownstreamAPI._instance = None
    test_env = {
        "DOWNSTREAMAPI_TOKEN": "abc",
        "DOWNSTREAMAPI_SECRET": "shhh",
    }

    with pytest.raises(ValueError, match="DOWNSTREAMAPI_URL is required"):
        await DownstreamAPI.from_env(test_env)


async def test_skips_internal_fields():
    DownstreamAPI._instance = None
    test_env = {
        "DOWNSTREAMAPI_TOKEN": "abc",
        "DOWNSTREAMAPI_URL": "https://x.com",
        "DOWNSTREAMAPI_SECRET": "shhh",
        "DOWNSTREAMAPI__INTERNAL": "hacked",
    }

    block = await DownstreamAPI.from_env(test_env)
    assert block.token == "abc"
    assert block._internal == "should not load"  # not overridden


async def test_instance_cached():
    DownstreamAPI._instance = None

    test_env_a = {
        "DOWNSTREAMAPI_TOKEN": "a",
        "DOWNSTREAMAPI_URL": "a",
        "DOWNSTREAMAPI_SECRET": "a",
    }

    test_env_b = {
        "DOWNSTREAMAPI_TOKEN": "b",
        "DOWNSTREAMAPI_URL": "b",
        "DOWNSTREAMAPI_SECRET": "b",
    }

    a = await DownstreamAPI.from_env(test_env_a)
    b = await DownstreamAPI.from_env(test_env_b)
    assert a is b


async def test_loads_block_if_env_name_provided():
    DownstreamAPI._instance = None

    class FakeBlock(DownstreamAPI):
        @classmethod
        async def load(cls, name):
            return cls(token="loaded", url="https://loaded", secret=SecretStr("lol"))

    test_env = {"FAKEBLOCK_CREDENTIAL_BLOCK_NAME": "something"}

    result = await FakeBlock.from_env(test_env)
    assert result.token == "loaded"


async def test_instances_are_isolated_per_subclass():
    class A(EnvBlock):
        token: str

    class B(EnvBlock):
        token: str

    reset_env_and_instance(A)
    reset_env_and_instance(B)

    os.environ["A_TOKEN"] = "aaa"
    os.environ["B_TOKEN"] = "bbb"

    a = await A.from_env()
    b = await B.from_env()

    assert a.token == "aaa"
    assert b.token == "bbb"
    assert a is not b


async def test_subclass_with_explicit_prefix():
    class ConfiguredPrefix(EnvBlock):
        prefix: str = "CustomPrefix"
        token: str

    test_env = {
        "CUSTOMPREFIX_TOKEN": "expected",
        "CONFIGUREDPREFIX_TOKEN": "should_be_ignored",
    }

    block = await ConfiguredPrefix.from_env(test_env)
    assert block.token == "expected"


async def test_successful_casting_of_standard_type():
    test_env = {
        "DOWNSTREAMAPI_TOKEN": "abc",
        "DOWNSTREAMAPI_URL": "https://x.com",
        "DOWNSTREAMAPI_SECRET": "shhh",
        "DOWNSTREAMAPI_RETRIES": "7",
    }

    block = await DownstreamAPI.from_env(test_env)
    assert block.retries == 7
    assert isinstance(block.retries, int)


async def test_unsuccessful_casting_raises_value_error():
    DownstreamAPI._instance = None
    test_env = {
        "DOWNSTREAMAPI_TOKEN": "abc",
        "DOWNSTREAMAPI_URL": "https://x.com",
        "DOWNSTREAMAPI_SECRET": "shhh",
        "DOWNSTREAMAPI_RETRIES": "notanint",
    }

    with pytest.raises(ValueError, match="DOWNSTREAMAPI_RETRIES.*int"):
        await DownstreamAPI.from_env(test_env)


async def test_secretstr_field_is_wrapped_correctly():
    reset_env_and_instance(DownstreamAPI)
    os.environ["DOWNSTREAMAPI_TOKEN"] = "abc"
    os.environ["DOWNSTREAMAPI_URL"] = "https://x.com"
    os.environ["DOWNSTREAMAPI_SECRET"] = "supersecret"

    block = await DownstreamAPI.from_env()
    assert isinstance(block.secret, SecretStr)
    assert block.secret.get_secret_value() == "supersecret"


async def test_union_field_casting_with_optional():
    class UnionBlock(EnvBlock):
        prefix: str = "UnionTest"
        retries: int | str  # Union[int, str]
        region: None | str  # Optional[str]
        token: Optional[str]
        url: Union[str, int]
        secret: SecretStr

    test_env = {
        "UNIONTEST_RETRIES": "42",  # should cast to int
        "UNIONTEST_REGION": "au-east",  # should remain as str
        "UNIONTEST_TOKEN": "abc",
        "UNIONTEST_URL": "https://x.com",
        "UNIONTEST_SECRET": "shhh",
    }

    block = await UnionBlock.from_env(test_env)

    assert isinstance(block.retries, int)
    assert block.retries == 42

    assert isinstance(block.region, str)
    assert block.region == "au-east"
