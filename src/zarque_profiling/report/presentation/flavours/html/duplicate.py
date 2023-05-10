import pandas as pd
import polars as pol

from zarque_profiling.report.presentation.core.duplicate import Duplicate
from zarque_profiling.report.presentation.flavours.html import templates


def to_html(df: pd.DataFrame) -> str:
    if isinstance(df, pol.DataFrame):
        df = df.to_pandas() 
    html = df.to_html(
        classes="duplicate table table-striped",
    )
    if df.empty:
        html = html.replace(
            "<tbody>",
            f"<tbody><tr><td colspan={len(df.columns) + 1}>Dataset does not contain duplicate rows.</td></tr>",
        )
    return html


class HTMLDuplicate(Duplicate):
    def render(self) -> str:
        duplicate_html = to_html(self.content["duplicate"])
        return templates.template("duplicate.html").render(
            **self.content, duplicate_html=duplicate_html
        )
