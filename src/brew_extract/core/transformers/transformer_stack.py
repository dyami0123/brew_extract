from copy import deepcopy
from typing import Generic

from ..type_vars import S, T
from .abstract_data_source import AbstractDataSource
from .abstract_transformer import AbstractTransformer


class TransformerStack(AbstractDataSource[T, S], Generic[T, S]):
    source: AbstractDataSource[T, S]
    transformers: list[AbstractTransformer[T, S]]

    def __init__(self, source: AbstractDataSource, transformers: list[AbstractTransformer[T, S]]):
        self.source = source
        self.transformers = transformers

    @classmethod
    def empty(cls, source: AbstractDataSource) -> "TransformerStack":
        return cls(source=source, transformers=[])

    def generate(self) -> T:
        data = self.source.generate()
        for transformer in self.transformers:
            data = transformer.transform(data)
        return data

    def with_new_transformer(self, transformer: AbstractTransformer) -> "TransformerStack":
        new_obj = deepcopy(self)
        new_obj.transformers.append(transformer)
        return new_obj

    @property
    def id(self) -> str:
        return str(len(self.transformers))

    # FIXME: not sure why this is giving a type error
    @property
    def schema(self) -> S:
        schema = self.source.schema
        for transformer in self.transformers:
            schema = transformer.transform_schema(schema)
        return schema

    def validate_schema(self) -> bool:
        self.schema
        return True
