"""
Type converters and generators for MTGJSON models.
"""

from __future__ import annotations

import re
import typing
from typing import TYPE_CHECKING, Any, ClassVar, get_args, get_origin

from pydantic import BaseModel
from typing_extensions import Required  # noqa: UP035

from ._typing import TypedDictUtils, is_union_type

try:
    import polars as pl

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None  # type: ignore[assignment]

if TYPE_CHECKING:
    from polars.datatypes import DataType, Struct


class PolarsConverter:
    """Converts between Python/Pydantic types and Polars types."""

    @classmethod
    def python_to_polars(cls, python_type: Any) -> DataType:
        """Map Python type to Polars dtype."""
        if not POLARS_AVAILABLE:
            raise ImportError("Polars required")

        origin = get_origin(python_type)
        args = get_args(python_type)

        # Handle Union / Optional
        if is_union_type(python_type):
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) == 1:
                return cls.python_to_polars(non_none[0])
            return pl.String  # type: ignore[return-value]

        # Handle list
        if origin is list:
            inner = cls.python_to_polars(args[0]) if args else pl.String
            return pl.List(inner)

        # Handle TypedDict
        if TypedDictUtils.is_typeddict(python_type):
            return cls.typeddict_to_struct(python_type)

        # Handle Pydantic model
        if isinstance(python_type, type) and issubclass(python_type, BaseModel):
            return cls.model_to_struct(python_type)

        # Primitives
        if python_type is str:
            return pl.String  # type: ignore[return-value]
        if python_type is int:
            return pl.Int64  # type: ignore[return-value]
        if python_type is float:
            return pl.Float64  # type: ignore[return-value]
        if python_type is bool:
            return pl.Boolean  # type: ignore[return-value]

        return pl.String  # type: ignore[return-value]

    @classmethod
    def typeddict_to_struct(cls, td: type) -> Struct:
        """Convert TypedDict to Polars Struct."""
        if not POLARS_AVAILABLE:
            raise ImportError("Polars required")
        fields = []
        for name in sorted(TypedDictUtils.get_fields(td).keys()):
            field_type = TypedDictUtils.get_fields(td)[name]
            fields.append(pl.Field(name, cls.python_to_polars(field_type)))
        return pl.Struct(fields)

    @classmethod
    def model_to_struct(cls, model: type[BaseModel]) -> Struct:
        """Convert Pydantic model to Polars Struct."""
        if not POLARS_AVAILABLE:
            raise ImportError("Polars required")
        fields = []
        for name in sorted(model.model_fields.keys()):
            info = model.model_fields[name]
            output_name = info.alias or name
            fields.append(pl.Field(output_name, cls.python_to_polars(info.annotation)))
        return pl.Struct(fields)


class TypeScriptGenerator:
    """Generates TypeScript interfaces from Python types."""

    PRIMITIVE_MAP: ClassVar[dict[type, str]] = {
        str: "string",
        int: "number",
        float: "number",
        bool: "boolean",
        type(None): "null",
    }

    @classmethod
    def python_to_ts(cls, python_type: Any) -> str:
        """Convert Python type to TypeScript type string."""
        # Handle ForwardRef
        if isinstance(python_type, typing.ForwardRef):
            s = python_type.__forward_arg__
            m = {
                "str": "string",
                "int": "number",
                "float": "number",
                "bool": "boolean",
                "None": "null",
            }
            if s in m:
                return m[s]
            if s.startswith("list["):
                inner = s[5:-1]
                return f"{m.get(inner, inner)}[]"
            if s.startswith("Required["):
                inner = s[9:-1]
                return m.get(inner, inner)
            return s

        origin = get_origin(python_type)
        args = get_args(python_type)

        # Handle Union
        if is_union_type(python_type):
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) == 1:
                return cls.python_to_ts(non_none[0])
            return " | ".join(cls.python_to_ts(a) for a in non_none)

        if origin is Required:
            return cls.python_to_ts(args[0]) if args else "any"

        if origin is list:
            return f"{cls.python_to_ts(args[0])}[]" if args else "any[]"

        if origin is dict:
            if args and len(args) == 2:
                return f"Record<{cls.python_to_ts(args[0])}, {cls.python_to_ts(args[1])}>"
            return "Record<string, any>"

        if TypedDictUtils.is_typeddict(python_type):
            return python_type.__name__  # type: ignore[no-any-return]

        if isinstance(python_type, type) and issubclass(python_type, BaseModel):
            return getattr(python_type, "__ts_name__", python_type.__name__)

        return cls.PRIMITIVE_MAP.get(python_type, "any")

    _TRANSLATIONS_KEY_MAP: ClassVar[dict[str, str]] = {
        "AncientGreek": "Ancient Greek",
        "ChineseSimplified": "Chinese Simplified",
        "ChineseTraditional": "Chinese Traditional",
        "PortugueseBrazil": "Portuguese (Brazil)",
    }

    @classmethod
    def from_typeddict(cls, td: type, indent: str = "  ") -> str:
        """Generate TypeScript type from TypedDict."""
        lines = [f"export type {td.__name__} = {{"]
        fields = TypedDictUtils.get_fields(td)
        is_translations = td.__name__ == "Translations"
        for name in sorted(fields.keys()):
            required = TypedDictUtils.is_field_required(td, name)
            ts_type = cls.python_to_ts(fields[name])
            opt = "" if required else "?"
            if is_translations:
                display_name = cls._TRANSLATIONS_KEY_MAP.get(name, name)
                if display_name != name:
                    lines.append(f'{indent}"{display_name}"{opt}: {ts_type};')
                else:
                    lines.append(f"{indent}{display_name}{opt}: {ts_type};")
            else:
                lines.append(f"{indent}{name}{opt}: {ts_type};")
        lines.append("};")
        return "\n".join(lines)

    _OUTPUT_CONTRACT: ClassVar[dict[str, dict[str, str]]] = {
        "CardAtomic": {"purchaseUrls": "required"},
        "CardSet": {"purchaseUrls": "required"},
        "CardDeck": {"purchaseUrls": "required", "isFoil": "required"},
        "DeckSet": {"releaseDate": "required", "sealedProductUuids": "nullable"},
        "DeckList": {"releaseDate": "required"},
        "Deck": {"releaseDate": "required", "sealedProductUuids": "nullable"},
    }

    @classmethod
    def from_model(cls, model: type[BaseModel], indent: str = "  ") -> str:
        """Generate TypeScript type from Pydantic model."""
        import sys

        ts_name = getattr(model, "__ts_name__", model.__name__)
        lines = [f"export type {ts_name} = {{"]

        # Get resolved hints
        try:
            module = sys.modules.get(model.__module__, None)
            globalns = getattr(module, "__dict__", {}) if module else {}
            hints = typing.get_type_hints(model, globalns=globalns)
        except Exception:
            hints = {}

        contract = cls._OUTPUT_CONTRACT.get(ts_name, {})
        sorted_fields = sorted(model.model_fields.items(), key=lambda x: x[1].alias or x[0])

        for name, info in sorted_fields:
            output_name = info.alias or name
            annotation = hints.get(name, info.annotation)
            contract_mode = contract.get(output_name)

            # Determine optionality
            if contract_mode in ("required", "nullable"):
                is_optional = False
            else:
                is_optional = info.default is None

            if is_union_type(annotation):
                args = get_args(annotation)
                non_none = [a for a in args if a is not type(None)]
                if len(non_none) == 1:
                    annotation = non_none[0]

            ts_type = cls.python_to_ts(annotation)

            # For nullable contract, re-add | null
            if contract_mode == "nullable":
                ts_type = f"{ts_type} | null"

            opt = "?" if is_optional else ""
            lines.append(f"{indent}{output_name}{opt}: {ts_type};")

        lines.append("};")
        return "\n".join(lines)

    @classmethod
    def from_file_model(cls, model: type[BaseModel]) -> str:
        """Generate one-liner TypeScript type for file wrapper models."""
        import sys

        # Get resolved hints for the data field
        try:
            module = sys.modules.get(model.__module__, None)
            globalns = getattr(module, "__dict__", {}) if module else {}
            hints = typing.get_type_hints(model, globalns=globalns)
        except Exception:
            hints = {}

        data_annotation = hints.get("data", model.model_fields["data"].annotation)
        data_ts = cls.python_to_ts(data_annotation)
        ts_name = getattr(model, "__ts_name__", model.__name__)
        return f"export type {ts_name} = {{ meta: Meta; data: {data_ts}; }};"


class MarkdownDocGenerator:
    """Generates VitePress markdown documentation pages from Pydantic model metadata."""

    @classmethod
    def from_model(cls, model: type[BaseModel]) -> str:
        """Generate a complete VitePress markdown page from model doc metadata."""
        import sys

        title = getattr(model, "__doc_title__", model.__name__)
        desc = getattr(model, "__doc_desc__", "")
        parent = getattr(model, "__doc_parent__", "")
        enum = getattr(model, "__doc_enum__", "")
        keywords = getattr(model, "__doc_keywords__", "")
        extra_sections = getattr(model, "__doc_extra__", "")
        ts_name = getattr(model, "__ts_name__", model.__name__)

        # Plain-text version for meta tags (strip markdown links)
        desc_plain = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", desc)

        lines: list[str] = []

        # --- Frontmatter ---
        lines.append("---")
        lines.append(f"title: {title}")
        if enum:
            lines.append(f"enum: {enum}")
        lines.append("head:")
        lines.append("  - - meta")
        lines.append("    - property: og:title")
        lines.append(f"      content: {title}")
        lines.append("  - - meta")
        lines.append("    - name: description")
        lines.append(f"      content: {desc_plain}")
        lines.append("  - - meta")
        lines.append("    - property: og:description")
        lines.append(f"      content: {desc_plain}")
        if keywords:
            lines.append("  - - meta")
            lines.append("    - name: keywords")
            lines.append(f"      content: {keywords}")
        lines.append("---")
        lines.append("")

        # --- Title and description ---
        lines.append(f"# {title}")
        lines.append("")
        lines.append(desc)
        lines.append("")
        if parent:
            lines.append(f"- {parent}")
        lines.append("")

        # --- Extra content sections (tips, notes, etc.) ---
        if extra_sections:
            lines.append(extra_sections)
            lines.append("")

        # --- TypeScript Model section ---
        lines.append("## TypeScript Model")
        lines.append("")
        lines.append("::: details Toggle Model {open}")
        lines.append("")
        lines.append(f"<<< @/public/types/{ts_name}.ts{{TypeScript}}")
        lines.append("")
        lines.append(":::")
        lines.append("")

        # --- Model Properties ---
        lines.append("## Model Properties")
        lines.append("")

        # Resolve type hints
        try:
            module = sys.modules.get(model.__module__, None)
            globalns = getattr(module, "__dict__", {}) if module else {}
            hints = typing.get_type_hints(model, globalns=globalns)
        except Exception:
            hints = {}

        # Per-model field overrides (for inherited fields with model-specific metadata)
        overrides: dict[str, dict[str, Any]] = getattr(model, "__doc_field_overrides__", {})

        # Sort fields by output name (alias); only emit fields with "introduced" metadata
        sorted_fields = sorted(
            model.model_fields.items(),
            key=lambda x: x[1].alias or x[0],
        )

        for name, info in sorted_fields:
            extra = info.json_schema_extra or {}
            output_name = info.alias or name
            field_ov = overrides.get(output_name, {})

            if "introduced" not in extra and "introduced" not in field_ov:
                continue

            annotation = hints.get(name, info.annotation)

            description = field_ov.get("description", info.description or "")
            introduced = field_ov.get("introduced", extra.get("introduced", ""))
            enum_key = field_ov.get("enum_key", extra.get("enum_key", ""))
            is_optional = field_ov.get("optional", extra.get("optional", False))
            is_deprecated = field_ov.get("deprecated", extra.get("deprecated", False))
            deprecated_msg = field_ov.get("deprecated_msg", extra.get("deprecated_msg", ""))

            # Get the TS type string.
            # When "optional" badge is shown, strip "| null" from type
            # (the badge conveys "may be absent"; | null is for "present but nullable")
            type_override = field_ov.get("type_override", extra.get("type_override", ""))
            if type_override:
                ts_type = type_override
            else:
                ts_type = cls._field_ts_type(annotation, strip_null=is_optional)
            example = field_ov.get("example", extra.get("example", ""))

            # Build badge string
            badges = ""
            if is_deprecated:
                badges += ' <DocBadge type="danger" text="deprecated" />'
            if is_optional:
                badges += ' <DocBadge type="warning" text="optional" />'

            lines.append(f"> ### {output_name}{badges}")
            lines.append(">")
            lines.append(f"> {description}")

            if deprecated_msg:
                lines.append(">")
                lines.append(f"> _{deprecated_msg}_")

            lines.append(">")
            lines.append(f"> - **Type:** `{ts_type}`")
            if example:
                lines.append(f"> - **Example:** `{example}`")
            if enum_key:
                lines.append(f"> - <ExampleField type='{enum_key}'/>")
            lines.append(f"> - **Introduced:** `{introduced}`")
            lines.append("")

        return "\n".join(lines)

    @classmethod
    def _field_ts_type(cls, annotation: Any, strip_null: bool = False) -> str:
        """Get the TS type for a field, preserving null union for Optional types.

        Args:
            annotation: The Python type annotation.
            strip_null: If True, strip ``| null`` from the result.
                Used when the "optional" badge already conveys absence.
        """
        if is_union_type(annotation):
            args = get_args(annotation)
            non_none = [a for a in args if a is not type(None)]
            has_none = len(non_none) < len(args)
            if len(non_none) == 1:
                base = TypeScriptGenerator.python_to_ts(non_none[0])
                if strip_null or not has_none:
                    return base
                return f"{base} | null"
            parts = [TypeScriptGenerator.python_to_ts(a) for a in non_none]
            joined = " | ".join(parts)
            if strip_null or not has_none:
                return joined
            return f"{joined} | null"
        return TypeScriptGenerator.python_to_ts(annotation)

    @classmethod
    def from_typeddict(cls, td: type) -> str:
        """Generate a complete VitePress markdown page from TypedDict doc metadata."""
        title = getattr(td, "__doc_title__", td.__name__)
        desc = getattr(td, "__doc_desc__", "")
        parent = getattr(td, "__doc_parent__", "")
        enum = getattr(td, "__doc_enum__", "")
        keywords = getattr(td, "__doc_keywords__", "")
        extra_sections = getattr(td, "__doc_extra__", "")

        # Plain-text version for meta tags (strip markdown links)
        desc_plain = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", desc)

        lines: list[str] = []

        # --- Frontmatter ---
        lines.append("---")
        lines.append(f"title: {title}")
        if enum:
            lines.append(f"enum: {enum}")
        lines.append("head:")
        lines.append("  - - meta")
        lines.append("    - property: og:title")
        lines.append(f"      content: {title}")
        lines.append("  - - meta")
        lines.append("    - name: description")
        lines.append(f"      content: {desc_plain}")
        lines.append("  - - meta")
        lines.append("    - property: og:description")
        lines.append(f"      content: {desc_plain}")
        if keywords:
            lines.append("  - - meta")
            lines.append("    - name: keywords")
            lines.append(f"      content: {keywords}")
        lines.append("---")
        lines.append("")

        # --- Title and description ---
        lines.append(f"# {title}")
        lines.append("")
        lines.append(desc)
        lines.append("")
        if parent:
            lines.append(f"- {parent}")
        lines.append("")

        # --- Extra content sections (tips, notes, etc.) ---
        if extra_sections:
            lines.append(extra_sections)
            lines.append("")

        # --- TypeScript Model section ---
        lines.append("## TypeScript Model")
        lines.append("")
        lines.append("::: details Toggle Model {open}")
        lines.append("")
        lines.append(f"<<< @/public/types/{td.__name__}.ts{{TypeScript}}")
        lines.append("")
        lines.append(":::")
        lines.append("")

        # --- Model Properties ---
        lines.append("## Model Properties")
        lines.append("")

        field_docs: dict[str, dict[str, Any]] = getattr(td, "__field_docs__", {})
        if not field_docs:
            return "\n".join(lines)

        fields = TypedDictUtils.get_fields(td)

        # Translations uses display names for headers
        is_translations = td.__name__ == "Translations"
        translations_map = TypeScriptGenerator._TRANSLATIONS_KEY_MAP

        # Build (display_name, field_name) pairs for sorting
        sort_pairs = []
        for name in field_docs:
            if is_translations:
                display = translations_map.get(name, name)
            else:
                display = name
            sort_pairs.append((display, name))
        sort_pairs.sort(key=lambda x: x[0])

        for display_name, name in sort_pairs:
            doc = field_docs[name]
            description = doc.get("description", "")
            introduced = doc.get("introduced", "")
            is_optional = doc.get("optional", False)
            is_deprecated = doc.get("deprecated", False)
            deprecated_msg = doc.get("deprecated_msg", "")
            example = doc.get("example", "")
            enum_key = doc.get("enum_key", "")
            type_override = doc.get("type_override", "")

            # Get the TS type string
            if type_override:
                ts_type = type_override
            elif name in fields:
                ts_type = cls._field_ts_type(fields[name], strip_null=is_optional)
            else:
                ts_type = "any"

            # Build badge string
            badges = ""
            if is_deprecated:
                badges += ' <DocBadge type="danger" text="deprecated" />'
            if is_optional:
                badges += ' <DocBadge type="warning" text="optional" />'

            lines.append(f"> ### {display_name}{badges}")
            lines.append(">")
            lines.append(f"> {description}")

            if deprecated_msg:
                lines.append(">")
                lines.append(f"> _{deprecated_msg}_")

            lines.append(">")
            lines.append(f"> - **Type:** `{ts_type}`")
            if example:
                lines.append(f"> - **Example:** `{example}`")
            if enum_key:
                lines.append(f"> - <ExampleField type='{enum_key}'/>")
            lines.append(f"> - **Introduced:** `{introduced}`")
            lines.append("")

        return "\n".join(lines)

    @classmethod
    def slug_from_title(cls, title: str) -> str:
        """Convert a doc title to a URL slug (e.g. 'Deck List' -> 'deck-list')."""
        return re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
