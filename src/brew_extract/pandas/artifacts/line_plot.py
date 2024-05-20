from typing import Callable

import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.figure import Figure

from .matplotlib_generic import GenericMatplotlibVisualization


class LinePlot(GenericMatplotlibVisualization):
    @staticmethod
    def plot(data: pd.DataFrame) -> Figure:
        fig, ax = plt.subplots()

        data.plot(ax=ax)

        return fig

    @property
    def plot_function(self) -> Callable[[pd.DataFrame], Figure]:
        return self.plot
