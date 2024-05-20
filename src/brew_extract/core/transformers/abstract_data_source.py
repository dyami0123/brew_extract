from abc import ABC, abstractmethod
from typing import Generic

from ..base_class import BaseClass
from ..type_vars import S, T


# TODO: consider making this a protocol?
class AbstractDataSource(ABC, Generic[T, S], BaseClass):

    @abstractmethod
    def generate(self) -> T:
        pass

    @property
    def schema(self) -> S:
        self.logger.warning("Schema not implemented for this data source, infering schema instead")
        return self.infer_schema(self.generate())

    @abstractmethod
    def infer_schema(self, data: T) -> S:
        pass
