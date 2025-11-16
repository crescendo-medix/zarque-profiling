from typing imfrom typing import Tuple

import polars as pol

from zarque_profiling.config import Settings
from zarque_profiling.model.summary_algorithms import describe_supported, series_hashable


@describe_supported.register
@series_hashable
def polars_describe_supported(
    config: Settings, series: pol.Series, series_description: dict
) -> Tuple[Settings, pol.Series, dict]:
    """Describe a supported series.

    Args:
        config: report Settings object
        series: The Series to describe.
        series_description: The dict containing the series description so far.

    Returns:
        A dict containing calculated series description values.
    """

    # number of non-NaN observations in the Series
    count = series_description["count"]

    value_counts = series_description["value_counts_without_nan"]
    distinct_count = len(value_counts)
    #unique_count = value_counts.where(value_counts == 1).count()
    #unique_count = len(value_counts["counts"] == 1)    # TODO unique_count OK?
    unique_count = len(value_counts["count"] == 1)    # TODO unique_count OK?

    stats = {
        "n_distinct": distinct_count,
        "p_distinct": distinct_count / count if count > 0 else 0,
        "is_unique": unique_count == count and count > 0,
        "n_unique": unique_count,
        "p_unique": unique_count / count if count > 0 else 0,
    }
    stats.update(series_description)

    return config, series, stats
