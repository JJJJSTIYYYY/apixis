"""Public Apixis API and version information."""

from apixis.core import *
from apixis.core import __all__ as _core_exports
from apixis._version import __version__


VERSION = __version__

__all__ = [*_core_exports, "VERSION", "__version__"]
