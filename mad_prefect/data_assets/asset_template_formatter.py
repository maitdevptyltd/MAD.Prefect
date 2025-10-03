from __future__ import annotations

import inspect
import string
from typing import Any

from mad_prefect.data_assets.data_asset import DataAsset


class AssetTemplateFormatter:
    """Render data asset templates using bound argument context."""

    def __init__(
        self,
        format_args: tuple[Any, ...],
        bound_arguments: inspect.BoundArguments,
    ) -> None:
        self._format_args = format_args
        self._formatter = string.Formatter()
        self._format_kwargs = self._prepare_format_kwargs(bound_arguments)

    @property
    def format_kwargs(self) -> dict[str, Any]:
        """Return a shallow copy of the computed keyword arguments."""
        return dict(self._format_kwargs)

    def format(self, template: str | None) -> str | None:
        """Format ``template`` using positional and keyword arguments.

        The method resolves nested placeholders until the rendered string contains
        no additional fields. If a placeholder is missing, a ``KeyError`` with a
        descriptive message is raised.
        """
        if template is None:
            return None

        formatted = template
        seen_templates: set[str] = set()

        while self._has_unresolved_fields(formatted):
            if formatted in seen_templates:
                break

            seen_templates.add(formatted)

            try:
                formatted = formatted.format(
                    *self._format_args,
                    **self._format_kwargs,
                )
            except KeyError as exc:
                missing_key = exc.args[0] if exc.args else "<unknown>"
                available_keys = ", ".join(sorted(self._format_kwargs.keys())) or "<none>"
                raise KeyError(
                    "Missing format key "
                    f"'{missing_key}' while formatting asset template '{template}'. "
                    f"Available keys: {available_keys}"
                ) from exc

        return formatted

    def _prepare_format_kwargs(
        self, bound_args: inspect.BoundArguments
    ) -> dict[str, Any]:
        format_kwargs: dict[str, Any] = dict(bound_args.arguments)
        nested_values: dict[str, Any] = {}
        seen_assets: set[int] = set()

        for value in format_kwargs.values():
            self._collect_nested_asset_arguments(value, nested_values, seen_assets)

        for key, value in nested_values.items():
            format_kwargs.setdefault(key, value)

        return format_kwargs

    def _collect_nested_asset_arguments(
        self,
        value: Any,
        accumulator: dict[str, Any],
        seen_assets: set[int],
    ) -> None:
        if not isinstance(value, DataAsset):
            return

        asset_id = id(value)
        if asset_id in seen_assets:
            return

        seen_assets.add(asset_id)

        nested_callable = value._callable
        nested_bound_args = nested_callable.get_bound_arguments()

        for key, nested_value in nested_bound_args.arguments.items():
            accumulator.setdefault(key, nested_value)
            self._collect_nested_asset_arguments(nested_value, accumulator, seen_assets)

    def _has_unresolved_fields(self, template: str) -> bool:
        for _, field_name, _, _ in self._formatter.parse(template):
            if field_name is not None:
                return True

        return False
