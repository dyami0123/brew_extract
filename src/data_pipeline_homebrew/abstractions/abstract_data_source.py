from abc import ABC, abstractmethod
from typing import Generic

from .base_class import BaseClass
from .type_vars import S, T


class AbstractDataSource(ABC, Generic[T, S], BaseClass):

    @abstractmethod
    def generate(self) -> T:
        pass

    @property
    @abstractmethod
    def schema(self) -> S:
        pass
