import pandas as pd


from ..pandas_schema import PandasSchema
from .pandas_data_source import AbstractPandasDataSource

class LocalCSVDataSource(AbstractPandasDataSource):
    path: str

    def generate(self) -> pd.DataFrame:
        return pd.read_csv(self.path)

    @property
    def schema(self) -> PandasSchema:
        df_head = pd.read_csv(self.path, nrows=5)
        schema = df_head.dtypes.to_dict()
        return PandasSchema(schema)
