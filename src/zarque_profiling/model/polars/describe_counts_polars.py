from typing import Tuple

import polars as pol
from typing import Tuple

import polars as pol

from zarque_profiling.config import Settings
from zarque_profiling.model.summary_algorithms import describe_counts


@describe_counts.register
def polars_describe_counts(
    config: Settings, series: pol.Series, summary: dict
) -> Tuple[Settings, pol.Series, dict]:
    """Counts the values in a series (with and without NaN, distinct).

    Args:
        config: report Settings object
        series: Series for which we want to calculate the values.
        summary: series' summary

    Returns:
        A dictionary with the count values (with and without NaN, distinct).
    """
    try:
        #value_counts_with_nan = series.value_counts(dropna=False)
        #_ = set(value_counts_with_nan.index)
        value_counts_with_nan = series.value_counts(sort=True)  # TODO value_counts_with_nan は DataFram になる
        hashable = True
    except Exception as e:  # noqa: E722
        hashable = False

    summary["hashable"] = hashable

    if hashable:        
        #value_counts_with_nan = value_counts_with_nan.filter(pol.col("counts") > 0)
        value_counts_with_nan = value_counts_with_nan.filter(pol.col("count") > 0)
        null_index = value_counts_with_nan.get_columns()[0].is_null()
        if null_index.any():
            n_missing = value_counts_with_nan.filter(null_index).get_columns()[1].sum()
            value_counts_without_nan = value_counts_with_nan.filter(~null_index)
        else:
            n_missing = 0
            value_counts_without_nan = value_counts_with_nan

        summary.update(
            {
                "value_counts_without_nan": value_counts_without_nan,
            }
        )

        try:
            sort_index = summary["value_counts_without_nan"].columns[0]
            summary["value_counts_index_sorted"] = summary["value_counts_without_nan"].sort(sort_index, descending=False)
            ordering = True
        except TypeError as e:
            ordering = False
    else:
        n_missing = series.filter(series.is_null()).len()
        ordering = False

    summary["ordering"] = ordering
    summary["n_missing"] = n_missing

    return config, series, summary
