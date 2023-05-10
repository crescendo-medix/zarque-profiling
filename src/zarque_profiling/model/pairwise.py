from typing import Any, List, Tuple

import pandas as pd
import polars as pol

from zarque_profiling.config import Settings
from zarque_profiling.visualisation.plot import scatter_pairwise


def get_scatter_tasks(
    config: Settings, continuous_variables: list
) -> List[Tuple[Any, Any]]:
    if not config.interactions.continuous:
        return []

    targets = config.interactions.targets
    if len(targets) == 0:
        targets = continuous_variables

    tasks = [(x, y) for y in continuous_variables for x in targets]
    return tasks


def get_scatter_plot(
    config: Settings, df: pd.DataFrame, x: Any, y: Any, continuous_variables: list
) -> str:

    if isinstance(df, pol.DataFrame):
        if x in continuous_variables:
            if y == x:
                df_temp = df[[x]].drop_nulls()
            else:
                df_temp = df[[x, y]].drop_nulls()
            return scatter_pairwise(config, df_temp[x], df_temp[y], x, y)
        else:
            return ""
    else:
        if x in continuous_variables:
            if y == x:
                df_temp = df[[x]].dropna()
            else:
                df_temp = df[[x, y]].dropna()
            return scatter_pairwise(config, df_temp[x], df_temp[y], x, y)
        else:
            return ""
