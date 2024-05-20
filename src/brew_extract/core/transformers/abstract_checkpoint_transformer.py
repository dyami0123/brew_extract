from abc import abstractmethod
from typing import Generic, Union

from ..type_vars import S, T
from .abstract_transformer import AbstractTransformer


class AbstractCheckpointTransformer(
    AbstractTransformer[T, S],
    Generic[T, S],
):

    pipeline_id: str
    stack_id: str

    def __init__(self, pipeline_id: str, stack_id: str):
        self.pipeline_id = pipeline_id
        self.stack_id = stack_id

    def transform(self, data: T) -> T:
        if self.checkpoint_exists():
            return self._read_checkpoint()
        else:
            self._write_checkpoint(data)
        return data

    def transform_schema(self, schema: Union[S, None]) -> S:
        if schema is None:
            return self.schema
        else:
            return schema

    @property
    @abstractmethod
    def schema(self) -> S:
        pass

    @abstractmethod
    def _write_checkpoint(self, data: T) -> None:
        pass

    @abstractmethod
    def _read_checkpoint(self) -> T:
        pass

    @abstractmethod
    def checkpoint_exists(self) -> bool:
        return False

    @abstractmethod
    def clear_checkpoint(self) -> None:
        pass
