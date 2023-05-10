from typing import Any, Dict, List, Union

from zarque_profiling.config import Style
from zarque_profiling.model.alerts import Alert
from zarque_profiling.report.presentation.core.item_renderer import ItemRenderer


class Alerts(ItemRenderer):
    def __init__(
        self, alerts: Union[List[Alert], Dict[str, List[Alert]]], style: Style, **kwargs
    ):
        super().__init__("alerts", {"alerts": alerts, "style": style}, **kwargs)

    def __repr__(self):
        return "Alerts"

    def render(self) -> Any:
        raise NotImplementedError()
