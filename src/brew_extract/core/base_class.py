import logging

import pydantic


class BaseClass(pydantic.BaseModel):

    class Config:
        arbitrary_types_allowed = True

    @property
    def logger(self):
        return logging
