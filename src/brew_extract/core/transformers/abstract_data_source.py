from abc import ABC, abstractmethod
from typing import Generic

from ..base_class import BaseClass
from ..type_vars import S, T


# TODO: consider making this a protocol?
class AbstractDataSource(ABC, BaseClass, Generic[T, S]):

    @abstractmethod
    def generate(self) -> T:
        pass

    # FIXME: schema is already defined in BaseClass via pydantic... find a new name?
    @property
    def schema(self) -> S:  # type: ignore
        raise NotImplementedError
