from zarque_profiling.report.presentation.core.correlation_table import CorrelationTable
from zarque_profiling.report.presentation.flavours.html import templates

import pandas as pd
import polars as pol

class HTMLCorrelationTable(CorrelationTable):
    def render(self) -> str:
        
        df = self.content["correlation_matrix"]
        if isinstance(df, pol.DataFrame):
            df = pd.DataFrame(df.to_dict(), index=df.columns)            

        correlation_matrix_html = df.to_html(
            classes="correlation-table table table-striped",
            float_format="{:.3f}".format,
        )
        return templates.template("correlation_table.html").render(
            **self.content, correlation_matrix_html=correlation_matrix_html
        )
