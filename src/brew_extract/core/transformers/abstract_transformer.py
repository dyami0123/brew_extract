from abc import ABC, abstractmethod
from typing import Generic

from ..base_class import BaseClass
from ..type_vars import S, T


class AbstractTransformer(ABC, BaseClass, Generic[T, S]):

    @abstractmethod
    def transform(self, data: T) -> T:
        raise NotImplementedError()

    @abstractmethod
    def transform_schema(self, schema: S) -> S:
        raise NotImplementedError()
