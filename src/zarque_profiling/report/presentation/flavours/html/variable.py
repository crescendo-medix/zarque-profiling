from zarque_profiling.report.presentation.core import Variable
from zarque_profiling.report.presentation.flavours.html import templates


class HTMLVariable(Variable):
    def render(self) -> str:
        return templates.template("variable.html").render(**self.content)
