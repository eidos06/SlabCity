from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lib.dsl import Chain
    from typing import Tuple
    from collections import Counter
    from lib.types import DataType


class ComponentAnalysisInfo:
    __slots__ = ["required_chains", "provided_chains", "data_type", "depth"]

    def __init__(self, required_chains: Tuple[Chain, ...], provided_chains: Tuple[Chain, ...], data_type: DataType | None = None, depth=0):
        self.required_chains: Tuple[Chain, ...] = required_chains
        self.provided_chains: Tuple[Chain, ...] = provided_chains
        self.data_type: DataType = data_type
        self.depth:int = depth
