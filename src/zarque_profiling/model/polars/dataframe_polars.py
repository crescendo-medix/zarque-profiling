import warnings

import polars as pol

from zarque_profiling.config import Settings
from zarque_profiling.model.dataframe import check_dataframe, preprocess
from zarque_profiling.utils.dataframe import rename_index


@check_dataframe.register
def polars_check_dataframe(df: pol.DataFrame) -> None:
    if not isinstance(df, pol.DataFrame):
        warnings.warn("df is not of type pandas.DataFrame")


@preprocess.register
def polars_preprocess(config: Settings, df: pol.DataFrame) -> pol.DataFrame:
    """Preprocess the dataframe

    Args:
        config: report Settings object
        df: the pandas DataFrame

    Returns:
        The preprocessed DataFrame
    """

    return df
