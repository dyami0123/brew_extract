from typing import Callable

import pandas as pd
from matplotlib.figure import Figure

from brew_extract.core import AbstractArtifactGenerator

from ..pandas_schema import PandasSchema


class GenericMatplotlibVisualization(AbstractArtifactGenerator[pd.DataFrame, PandasSchema, Figure]):
    @property
    def plot_function(self) -> Callable[[pd.DataFrame], Figure]:
        raise NotImplementedError

    def create_artifact(self, data: pd.DataFrame) -> Figure:
        return self.plot_function(data)
