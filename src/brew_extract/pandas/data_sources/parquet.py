import pandas as pd

from brew_extract.core import AbstractDataSource

from ..pandas_schema import PandasSchema


class LocalParquetDataSource(AbstractDataSource[pd.DataFrame, PandasSchema]):
    path: str

    def __init__(self, path: str):
        self.path = path

    def generate(self) -> pd.DataFrame:
        return pd.read_parquet(self.path)

    @property
    def schema(self) -> PandasSchema:
        df_head = pd.read_parquet(self.path, nrows=5)
        schema = df_head.dtypes.to_dict()
        return PandasSchema(schema)
