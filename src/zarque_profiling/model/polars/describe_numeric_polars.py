from typing import Any, Dict, Tuple

import numpy as np
import polars as pol

#from pandas.core.arrays.integer import _IntegerDtype as IntegerDtype
from zarque_profiling.config import Settings
from zarque_profiling.model.summary_algorithms import (
    chi_square,
    describe_numeric_1d,
    histogram_compute,
    series_handle_nulls,
    series_hashable,
)

def mad(arr: pol.Series) -> np.ndarray:
    """Median Absolute Deviation: a "Robust" version of standard deviation.
    Indices variability of the sample.
    https://en.wikipedia.org/wiki/Median_absolute_deviation
    """
    arr = arr.to_numpy()
    return np.median(np.abs(arr - np.median(arr)))

def numeric_stats_polars(series: pol.Series) -> Dict[str, Any]:
    return {
        "mean": series.mean(),
        "std": series.std(),
        "variance": series.var(),
        "min": series.min(),
        "max": series.max(),
        # Unbiased kurtosis obtained using Fisher's definition (kurtosis of normal == 0.0). Normalized by N-1.
        "kurtosis": series.kurtosis(),
        # Unbiased skew normalized by N-1
        "skewness": series.skew(),
        "sum": series.sum(),
    }

"""
def numeric_stats_numpy(
    present_values: np.ndarray, series: pol.Series, series_description: Dict[str, Any]
) -> Dict[str, Any]:
    vc = series_description["value_counts_without_nan"]
    index_values = vc.get_columns()[0].to_list()
    values = vc.get_columns()[1].to_list()

    # FIXME: can be performance optimized by using weights in std, var, kurt and skew...

    return {
        "mean": np.average(index_values, weights=values),
        "std": np.std(present_values, ddof=1),
        "variance": np.var(present_values, ddof=1),
        "min": np.min(index_values),
        "max": np.max(index_values),
        # Unbiased kurtosis obtained using Fisher's definition (kurtosis of normal == 0.0). Normalized by N-1.
        "kurtosis": series.kurtosis(),
        # Unbiased skew normalized by N-1
        "skewness": series.skew(),
        "sum": np.dot(index_values, values),
    }
"""

@describe_numeric_1d.register
@series_hashable
@series_handle_nulls
def polars_describe_numeric_1d(
    config: Settings, series: pol.Series, summary: dict
) -> Tuple[Settings, pol.Series, dict]:
    """Describe a numeric series.

    Args:
        config: report Settings object
        series: The Series to describe.
        summary: The dict containing the series description so far.

    Returns:
        A dict containing calculated series description values.
    """

    chi_squared_threshold = config.vars.num.chi_squared_threshold
    quantiles = config.vars.num.quantiles

    value_counts = summary["value_counts_without_nan"]

    negative_index = value_counts.get_columns()[0] < 0
    summary["n_negative"] = value_counts.get_columns()[0].filter(negative_index).len()
    summary["p_negative"] = summary["n_negative"] / summary["n"]

    summary["n_zeros"] = 0
    zero_index = value_counts.get_columns()[0] == 0
    if zero_index.any():
        zero_df = value_counts.filter(zero_index)
        summary["n_zeros"] = zero_df.get_columns()[1][0]

    stats = summary

    #if isinstance(series.dtype, IntegerDtype):
    if series.dtype == pol.Int64 or series.dtype == pol.Int32 or series.dtype == pol.Int16 or series.dtype == pol.Int8:
        infinity_index = value_counts.get_columns()[0].cast(pol.Float64).is_infinite()     # TODO Intでは is_infinite() できない！ 
        finite_values = value_counts.get_columns()[0].filter(value_counts.get_columns()[0].cast(pol.Float64).is_finite())     # TODO Intでは is_finite() できない！ 
    else:
        infinity_index = value_counts.get_columns()[0].is_infinite()
        finite_values = value_counts.get_columns()[0].filter(value_counts.get_columns()[0].is_finite())

    summary["n_infinite"] = value_counts.get_columns()[0].filter(infinity_index).len()
    stats.update(numeric_stats_polars(series))

    stats.update(
        {
            "mad": mad(series),
        }
    )

    if chi_squared_threshold > 0.0:
        stats["chi_squared"] = chi_square(finite_values)

    stats["range"] = stats["max"] - stats["min"]
    for quantile in quantiles:
        key = f"{quantile:.0%}"        
        value = series.quantile(quantile)
        stats.update(
            {
                key: value
            }
        )

    stats["iqr"] = stats["75%"] - stats["25%"]
    stats["cv"] = stats["std"] / stats["mean"] if stats["mean"] else np.NaN
    stats["p_zeros"] = stats["n_zeros"] / summary["n"]
    stats["p_infinite"] = summary["n_infinite"] / summary["n"]

    # TODO not suppot "monotonic"
    stats["monotonic_increase"] = series.is_sorted()
    stats["monotonic_decrease"] = series.is_sorted(descending=True)

    stats["monotonic_increase_strict"] = (
        stats["monotonic_increase"] and series.is_unique().all()
    )
    stats["monotonic_decrease_strict"] = (
        stats["monotonic_decrease"] and series.is_unique().all()
    )
    if summary["monotonic_increase_strict"]:
        stats["monotonic"] = 2
    elif summary["monotonic_decrease_strict"]:
        stats["monotonic"] = -2
    elif summary["monotonic_increase"]:
        stats["monotonic"] = 1
    elif summary["monotonic_decrease"]:
        stats["monotonic"] = -1
    else:
        stats["monotonic"] = 0

    stats.update(
        histogram_compute(
            config,
            value_counts.get_columns()[0].filter(~infinity_index).to_numpy(),
            summary["n_distinct"],
            weights=value_counts.get_columns()[1].filter(~infinity_index).to_numpy(),
        )
    )

    return config, series, stats
