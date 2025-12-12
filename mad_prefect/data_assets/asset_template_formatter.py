from __future__ import annotations

import inspect
import string
from typing import Any


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

    def format(self, template: str | None, allow_partial: bool = False) -> str | None:
        """Format ``template`` using positional and keyword arguments.

        When ``allow_partial`` is ``True`` missing placeholders are preserved so
        partially configured assets can render known values without raising.
        Otherwise behaviour matches ``str.format`` and missing keys raise.
        """
        if template is None:
            return None

        formatted = template
        seen_templates: set[str] = set()

        while self._has_unresolved_fields(formatted):
            if formatted in seen_templates:
                break

            seen_templates.add(formatted)

            # Perform a single formatting pass, switching between partial vs strict
            # behaviour depending on the caller. Partial mode uses a safe formatter
            # that keeps unresolved placeholders intact so initialization never raises.
            formatted = self._safe_format(
                formatted,
                template,
                allow_partial=allow_partial,
            )

        return formatted

    def _safe_format(
        self,
        template: str,
        original_template: str,
        allow_partial: bool,
    ) -> str:
        """Format a single pass of the template respecting ``allow_partial``."""
        if allow_partial:
            # ``format_map`` lets us intercept missing keys via _SafeFormatDict so we
            # can return placeholder proxies instead of raising.
            return template.format_map(_SafeFormatDict(self._format_kwargs))

        try:
            return template.format(*self._format_args, **self._format_kwargs)
        except KeyError as exc:
            missing_key = exc.args[0] if exc.args else "<unknown>"
            available_keys = ", ".join(sorted(self._format_kwargs.keys())) or "<none>"
            raise KeyError(
                "Missing format key "
                f"'{missing_key}' while formatting asset template "
                f"'{original_template}'. "
                f"Available keys: {available_keys}"
            ) from exc

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
        from mad_prefect.data_assets.data_asset import DataAsset

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


class _SafeFormatDict(dict[str, Any]):
    """dict that leaves missing template keys untouched."""

    def __missing__(self, key: str) -> str:
        return _Placeholder(key)


class _Placeholder(str):
    """String proxy that preserves dotted attribute/index access."""

    def __new__(cls, key: str) -> "_Placeholder":
        return super().__new__(cls, "{" + key + "}")

    def __getattr__(self, item: str) -> "_Placeholder":
        # Chaining attribute access (e.g. ".name") builds a new placeholder token.
        return _Placeholder(f"{self.strip('{}')}.{item}")

    def __getitem__(self, item: Any) -> "_Placeholder":
        # Support index access (e.g. "[0]") while keeping template markers intact.
        return _Placeholder(f"{self.strip('{}')}[{item}]")
