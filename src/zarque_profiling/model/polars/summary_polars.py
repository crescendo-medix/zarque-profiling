"""Compute statistical description of datasets."""

import multiprocessing
import multiprocessing.pool
from typing import Tuple

import numpy as np
import polars as pol
from tqdm import tqdm
from visions import VisionsTypeset

from zarque_profiling.config import Settings
from zarque_profiling.model.summarizer import BaseSummarizer
from zarque_profiling.model.summary import describe_1d, get_series_descriptions
from zarque_profiling.utils.dataframe import sort_column_names


@describe_1d.register
def polars_describe_1d(
    config: Settings,
    series: pol.Series,
    summarizer: BaseSummarizer,
    typeset: VisionsTypeset,
) -> dict:
    """Describe a series (infer the variable type, then calculate type-specific values).

    Args:
        config: report Settings object
        series: The Series to describe.
        summarizer: Summarizer object
        typeset: Typeset

    Returns:
        A Series containing calculated series description values.
    """

    # Make sure pol.NA is not in the series
    #series = series.fill_null(np.nan)

    # TODO config.infer_dtypes 無視
    # get `infer_dtypes` (bool) from config
    #if config.infer_dtypes:
    #    # Infer variable types
    #    vtype = typeset.infer_type(series)
    #    series = typeset.cast_to_inferred(series)
    #else:
    #    # Detect variable types from pandas dataframe (df.dtypes).
    #    # [new dtypes, changed using `astype` function are now considered]
    #    vtype = typeset.detect_type(series)

    dtype = str(series.dtype)

    if dtype == pol.List or dtype == pol.Struct:
        dtype = "ArrayType"

    vtype = {
            "Float32": "Numeric",
            "Float64": "Numeric",
            "Int8": "Numeric",
            "Int16": "Numeric",
            "Int32": "Numeric",
            "Int64": "Numeric",
            "UInt8": "Numeric",
            "UInt16": "Numeric",
            "UInt32": "Numeric",
            "UInt64": "Numeric",
            "Utf8": "Categorical",
            "Categorical": "Categorical",
            "ArrayType": "Categorical",
            "Boolean": "Boolean",
            "Date": "DateTime",
            "Time": "DateTime",
            "DateTime": "DateTime",
            "Datetime(tu='us', tz=None)": "DateTime",
            "Duration": "DateTime",
        }[dtype]        # TODO Data type Object?

    return summarizer.summarize(config, series, dtype=vtype)

@get_series_descriptions.register
def polars_get_series_descriptions(
    config: Settings,
    df: pol.DataFrame,
    summarizer: BaseSummarizer,
    typeset: VisionsTypeset,
    pbar: tqdm,
) -> dict:
    def multiprocess_1d(args: tuple) -> Tuple[str, dict]:
        """Wrapper to process series in parallel.

        Args:
            column: The name of the column.
            series: The series values.

        Returns:
            A tuple with column and the series description.
        """
        column, series = args        
        return column, describe_1d(config, series, summarizer, typeset)

    def get_items(df: pol.DataFrame) -> Tuple[str, pol.Series]:
        items = []
        names = df.columns
        series = df.get_columns()
        for i, name in enumerate(names):
            item = name, series[i]
            items.append(item)
        return items

    pool_size = config.pool_size

    # Multiprocessing of Describe 1D for each column
    if pool_size <= 0:
        pool_size = multiprocessing.cpu_count()

    #pool_size = 1   # TODO for debug

    #args = [(name, series) for name, series in df.items()]
    args = get_items(df)
    series_description = {}

    if pool_size == 1:
        for arg in args:
            pbar.set_postfix_str(f"Describe variable:{arg[0]}")
            column, description = multiprocess_1d(arg)
            series_description[column] = description
            pbar.update()
    else:
        # TODO: use `Pool` for Linux-based systems
        with multiprocessing.pool.ThreadPool(pool_size) as executor:
            for i, (column, description) in enumerate(
                executor.imap_unordered(multiprocess_1d, args)
            ):
                pbar.set_postfix_str(f"Describe variable:{column}")
                series_description[column] = description
                pbar.update()

        # Restore the original order
        series_description = {k: series_description[k] for k in df.columns}

    # Mapping from column name to variable type
    series_description = sort_column_names(series_description, config.sort)

    return series_description

