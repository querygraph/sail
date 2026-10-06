"""Native Spark Second String functions for Sail's extension host."""

import importlib
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Literal, cast

if TYPE_CHECKING:
    from ._native import BoundSecondString

__version__ = "0.1.0"


@dataclass(frozen=True, slots=True)
class ExtensionManifest:
    name: str = "second_string"
    version: str = __version__
    api_version: int = 1
    datafusion_version: str = "55.1.0"
    arrow_version: str = "59.3.0"
    placement: Literal["any"] = "any"
    relation_types: tuple[str, ...] = ()

    def to_wire(self) -> dict[str, object]:
        wire: dict[str, object] = asdict(self)
        wire["relation_types"] = list(self.relation_types)
        return wire


class SecondStringExtension:
    def manifest(self) -> dict[str, object]:
        """Expose compatibility metadata without loading the native library."""
        return ExtensionManifest().to_wire()

    def bind(self, session_id: str) -> "BoundSecondString":
        native = importlib.import_module("sail_second_string._native")
        constructor = cast("type[BoundSecondString]", native.BoundSecondString)
        return constructor(session_id)


extension = SecondStringExtension()

__all__ = ["ExtensionManifest", "SecondStringExtension", "__version__", "extension"]
