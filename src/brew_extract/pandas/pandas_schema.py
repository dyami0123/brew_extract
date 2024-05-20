from typing import Type


class PandasSchema:
    schema: dict[str, Type]

    def __init__(self, schema: dict[str, Type]):
        self.schema = schema
