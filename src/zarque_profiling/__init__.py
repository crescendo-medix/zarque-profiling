""" Main module of zarque-profiling. """

import importlib.util

from zarque_profiling.compare_reports import compare
from zarque_profiling.controller import pandas_decorator
from zarque_profiling.profile_report import ProfileReport
from zarque_profiling.version import __version__

import zarque_profiling.model.pandas

spec = importlib.util.find_spec("polars")
if spec is not None:
    import zarque_profiling.model.polars


__all__ = [
    "pandas_decorator",
    "ProfileReport",
    "__version__",
    "compare",
]
