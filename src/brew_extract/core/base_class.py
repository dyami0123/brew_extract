import logging
from dataclasses import dataclass


@dataclass  # TODO: finalize whether to use pydantic or dataclass
class BaseClass:

    class Config:
        arbitrary_types_allowed = True

    @property
    def logger(self):
        return logging
