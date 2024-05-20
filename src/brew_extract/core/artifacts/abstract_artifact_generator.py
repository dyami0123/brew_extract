from abc import ABC, abstractmethod
from typing import Generic

from ..transformers.abstract_transformer import AbstractTransformer
from ..type_vars import A, S, T
from .artifact_cache_singleton import ArtifactCacheSingleton


class AbstractArtifactGenerator(AbstractTransformer[T, S], ABC, Generic[T, S, A]):
    """
    An artifact generator is a transformer that creates an artifact from the data it receives.
    this for example, it could generate a summary dataframe, a plot, or any other form of output.

    The artifact generator class is type agnostic, and can be used to generate any type of artifact.
    It expects 3 type parameters:

    - T: the type of the data that is being transformed, and will be fed into the generator.
    this relates to the underlying transformer stack that the artifact generator is a part of.

    - S: the schema of the data that is being transformed. this is used to validate the schema of the data
    that is being transformed. this is less important for the artifact generator, but is still included for
    consistency. again, this relates to the underlying transformer stack that the artifact generator is a part of.

    - A: the type of the artifact that is being generated. this is the output of the artifact generator.
    this is the type that is cached in the artifact cache. for example, if the artifact generator is generating
    a matplotlib plot, this would be a matplotlib figure.

    (unintentionally, i've made a reference to the transportation
    security administration in the docstring for this class...)

    """

    cache: ArtifactCacheSingleton
    stack_id: str
    transformer_id: str

    def __init__(self, cache: ArtifactCacheSingleton, stack_id: str, transformer_id: str):
        self.cache = cache
        self.stack_id = stack_id
        self.transformer_id = transformer_id

    def transform(self, data: T) -> T:
        artifact = self.create_artifact(data)
        self.cache.cache(stack_id=self.stack_id, transformer_id=self.transformer_id, artifact=artifact)
        return data

    @abstractmethod
    def create_artifact(self, data: T) -> A:
        pass

    def transform_schema(self, schema: S) -> S:
        return schema
