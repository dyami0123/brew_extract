import pandas as pd

from brew_extract.core import AbstractDataSource

from ..pandas_schema import PandasSchema


class LocalCSVDataSource(AbstractDataSource[pd.DataFrame, PandasSchema]):
    path: str

    def __init__(self, path: str):
        self.path = path

    def generate(self) -> pd.DataFrame:
        return pd.read_csv(self.path)

    @property
    def schema(self) -> PandasSchema:
        df_head = pd.read_csv(self.path, nrows=5)
        schema = df_head.dtypes.to_dict()
        return PandasSchema(schema)
