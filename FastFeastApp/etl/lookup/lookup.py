
from __future__ import annotations
from abc import abstractmethod
from typing import Optional
import pandas as pd
from etl.components.data_flow_component import DataFlowComponent
from audit.audit import Audit
from registry.data_registry import DataRegistry


class LookUp(DataFlowComponent):

    def __init__(self, audit: Audit, registry: DataRegistry) -> None:
        super().__init__(audit=audit, registry=registry)

    @abstractmethod
    def do_task(self, data_frame_dict: dict) -> tuple[bool, list[str], dict, dict, Optional[pd.DataFrame]]:
        ...

    