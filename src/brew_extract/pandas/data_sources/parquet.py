import pandas as pd


from ..pandas_schema import PandasSchema
from .pandas_data_source import AbstractPandasDataSource


class LocalParquetDataSource(AbstractPandasDataSource):
    path: str


    def generate(self) -> pd.DataFrame:
        return pd.read_parquet(self.path)

    @property
    def schema(self) -> PandasSchema:
        df_head = pd.read_parquet(self.path, nrows=5)
        schema = df_head.dtypes.to_dict()
        return PandasSchema(schema)
