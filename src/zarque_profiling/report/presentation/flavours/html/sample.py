from zarque_profiling.report.presentation.core.sample import Sample
from zarque_profiling.report.presentation.flavours.html import templates

import polars as pol

class HTMLSample(Sample):
    def render(self) -> str:
        
        df = self.content["sample"]
        if isinstance(df, pol.DataFrame):
            df = df.to_pandas()    
        
        sample_html = df.to_html(
            classes="sample table table-striped"
        )
        return templates.template("sample.html").render(
            **self.content, sample_html=sample_html
        )
