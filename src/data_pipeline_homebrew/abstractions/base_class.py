from loguru import logger


class BaseClass:

    class Config:
        arbitrary_types_allowed = True

    @property
    def logger(self):
        return logger
