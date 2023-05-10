import os
from datetime import datetime
from typing import Tuple

import polars as pol

from zarque_profiling.config import Settings
from zarque_profiling.model.summary_algorithms import describe_file_1d, histogram_compute


def file_summary(series: pol.Series) -> dict:
    """

    Args:
        series: series to summarize

    Returns:

    """

    # Transform
    stats = series.map_dict(lambda x: os.stat(x))

    def convert_datetime(x: float) -> str:
        return datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S")

    # Transform some more
    summary = {
        "file_size": stats.map_dict(lambda x: x.st_size),
        "file_created_time": stats.map_dict(lambda x: x.st_ctime).map_dict(convert_datetime),
        "file_accessed_time": stats.map_dict(lambda x: x.st_atime).map_dict(convert_datetime),
        "file_modified_time": stats.map_dict(lambda x: x.st_mtime).map_dict(convert_datetime),
    }
    return summary


@describe_file_1d.register
def polars_describe_file_1d(
    config: Settings, series: pol.Series, summary: dict
) -> Tuple[Settings, pol.Series, dict]:
    if series.is_nan().any():
        raise ValueError("May not contain NaNs")
    if not hasattr(series, "str"):
        raise ValueError("series should have .str accessor")

    summary.update(file_summary(series))
    summary.update(
        histogram_compute(
            config,
            summary["file_size"],
            summary["file_size"].n_unique(),
            name="histogram_file_size",
        )
    )

    return config, series, summary
