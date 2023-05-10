import polars as pol

from zarque_profiling.config import Settings
from zarque_profiling.model.missing import missing_bar, missing_heatmap, missing_matrix
from zarque_profiling.visualisation.missing import (
    plot_missing_bar,
    plot_missing_heatmap,
    plot_missing_matrix,
)


@missing_bar.register
def polars_missing_bar(config: Settings, df: pol.DataFrame) -> str:
    return plot_missing_bar(config, df)


@missing_matrix.register
def polars_missing_matrix(config: Settings, df: pol.DataFrame) -> str:
    return plot_missing_matrix(config, df)


@missing_heatmap.register
def polars_missing_heatmap(config: Settings, df: pol.DataFrame) -> str:
    return plot_missing_heatmap(config, df)
