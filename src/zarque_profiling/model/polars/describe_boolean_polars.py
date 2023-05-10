from typing import Tuple

import polars as pol

from zarque_profiling.config import Settings
from zarque_profiling.model.polars.imbalance_polars import column_imbalance_score
from zarque_profiling.model.summary_algorithms import (
    describe_boolean_1d,
    series_hashable,
)


@describe_boolean_1d.register
@series_hashable
def polars_describe_boolean_1d(
    config: Settings, series: pol.Series, summary: dict
) -> Tuple[Settings, pol.Series, dict]:
    """Describe a boolean series.

    Args:
        config: report Settings object
        series: The Series to describe.
        summary: The dict containing the series description so far.

    Returns:
        A dict containing calculated series description values.
    """

    value_counts = summary["value_counts_without_nan"]
    summary.update({"top": value_counts.get_column(0)[0], "freq": value_counts.get_column(1)[0]})

    summary["imbalance"] = column_imbalance_score(value_counts, len(value_counts))

    return config, series, summary
