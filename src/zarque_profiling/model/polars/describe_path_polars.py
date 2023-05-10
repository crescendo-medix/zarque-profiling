import os
from typing import Tuple

import polars as pol

from zarque_profiling.config import Settings
from zarque_profiling.model.summary_algorithms import describe_path_1d


def path_summary(series: pol.Series) -> dict:
    """

    Args:
        series: series to summarize

    Returns:

    """

    # TODO: optimize using value counts
    summary = {
        "common_prefix": os.path.commonprefix(series.to_list())
        or "No common prefix",
        "stem_counts": series.map_dict(lambda x: os.path.splitext(x)[0]).value_counts(),
        "suffix_counts": series.map_dict(lambda x: os.path.splitext(x)[1]).value_counts(),
        "name_counts": series.map_dict(lambda x: os.path.basename(x)).value_counts(),
        "parent_counts": series.map_dict(lambda x: os.path.dirname(x)).value_counts(),
        "anchor_counts": series.map_dict(lambda x: os.path.splitdrive(x)[0]).value_counts(),
    }

    summary["n_stem_unique"] = len(summary["stem_counts"])
    summary["n_suffix_unique"] = len(summary["suffix_counts"])
    summary["n_name_unique"] = len(summary["name_counts"])
    summary["n_parent_unique"] = len(summary["parent_counts"])
    summary["n_anchor_unique"] = len(summary["anchor_counts"])

    return summary


@describe_path_1d.register
def polars_describe_path_1d(
    config: Settings, series: pol.Series, summary: dict
) -> Tuple[Settings, pol.Series, dict]:
    """Describe a path series.

    Args:
        config: report Settings object
        series: The Series to describe.
        summary: The dict containing the series description so far.

    Returns:
        A dict containing calculated series description values.
    """

    # Make sure we deal with strings (Issue #100)
    if series.is_nan().any():
        raise ValueError("May not contain NaNs")
    if not hasattr(series, "str"):
        raise ValueError("series should have .str accessor")

    summary.update(path_summary(series))

    return config, series, summary
