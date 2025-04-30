import types
from typing import (
    Any,
    Dict,
    Type,
    ClassVar,
    Optional,
    TypeVar,
    Union,
    get_args,
    get_origin,
)
from prefect.blocks.core import Block
import os
from pydantic import SecretStr
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined


T = TypeVar("T", bound="EnvBlock")


class EnvBlock(Block):
    # Optional prefix to customize environment variable names
    prefix: ClassVar[Optional[str]] = None

    # Singleton instance cache
    _instance: ClassVar[Optional[Any]] = None

    @staticmethod
    def retrieve_default_value(model_field: FieldInfo, env_var_name: str) -> Any:
        if model_field.default not in (None, PydanticUndefined):
            return model_field.default
        if not model_field.is_required():
            return None
        raise ValueError(f"Environment variable {env_var_name} is required.")

    @staticmethod
    def unwrap_types(field_type: Any) -> list[type]:
        origin = get_origin(field_type)
        args = get_args(field_type)

        if origin in (Union, types.UnionType):
            return [t for t in args if t is not type(None)]
        return [field_type]

    @staticmethod
    def apply_field_type(
        raw_value: str, field_types: list[type], env_var_name: str
    ) -> Any:
        for possible_type in field_types:
            try:
                if possible_type == SecretStr:
                    return SecretStr(raw_value)
                return possible_type(raw_value)
            except Exception:
                continue
        raise ValueError(
            f"Error converting {env_var_name} value to any of {field_types}"
        )

    @classmethod
    def resolve_prefix(cls) -> str:
        """
        Returns the resolved prefix for the subclass:
        - Use subclass-defined prefix if present
        - Otherwise fallback to class name
        - Returned value is upper cased
        """
        model_fields = cls.model_fields
        prefix_field = model_fields.get("prefix")
        default_prefix = (
            prefix_field.default if prefix_field else None
        )

        prefix = default_prefix or cls.__name__
        return prefix.upper()

    @classmethod
    async def from_env(cls: Type[T], env_vars: Optional[dict[str, str]] = None) -> T:
        """
        Load an instance of the subclass from environment variables.

        If the environment variable `<PREFIX>_CREDENTIAL_BLOCK_NAME` is set, the method
        loads the corresponding Prefect block using `cls.load()` and returns it.

        Otherwise, this method inspects the subclass's annotated attributes and attempts
        to construct the instance by reading each attribute's value from environment variables.
        Each environment variable must be named `<PREFIX>_<ATTRIBUTE>`, where `PREFIX` is
        either the class-level `prefix` or the subclass name, uppercased.

        Values are cast to the annotated types, including support for `SecretStr`. If a required
        environment variable is missing and no default is defined in the subclass, a `ValueError`
        is raised. The result is cached and reused on subsequent calls.

        Example:
            For a subclass `MyCreds`:

                class MyCreds(EnvBlock):
                    token: str
                    url: str

            Set the environment variables:
                MYCREDS_TOKEN=abc123
                MYCREDS_URL=https://example.com

            Then call:
                creds = await MyCreds.from_env()

        Returns:
            An instance of the subclass with attributes populated from environment variables.

        Raises:
            ValueError: If a required environment variable is missing or cannot
            be converted to the expected type.
        """
        # Return cached instance if already created
        if cls._instance is not None:
            return cls._instance

        # If no env_vars dict passed in use os.environ
        if env_vars is None:
            env_vars = dict(os.environ)

        resolved_prefix = cls.resolve_prefix()

        # Attempt to load a Prefect block by name from an environment variable
        block_name = env_vars.get(f"{resolved_prefix}_CREDENTIAL_BLOCK_NAME")
        if block_name is not None:
            block = await cls.load(block_name)  # type: ignore
            cls._instance = block
            return block

        # Otherwise, construct the block from environment variables
        field_values: Dict[str, Any] = {}

        for field, model_field in cls.model_fields.items():
            # Skip private/internal fields
            if field.startswith("_"):
                continue

            # Form the expected environment variable name
            env_var_name = f"{resolved_prefix}_{field.upper()}"
            raw_value = env_vars.get(env_var_name)

            # Use class-level default if env var is not set
            if raw_value is None:
                field_values[field] = cls.retrieve_default_value(
                    model_field, env_var_name
                )

            # Else cast the raw_value to the appropriate type
            else:
                # Extract the possible field types
                field_types = cls.unwrap_types(model_field.annotation)
                field_values[field] = cls.apply_field_type(
                    raw_value, field_types, env_var_name
                )

        # Instantiate and cache the block
        instance = cls(**field_values)
        cls._instance = instance
        return instance
