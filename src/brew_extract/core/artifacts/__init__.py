from .abstract_artifact_cache import AbstractArtifactCache  # noqa
from .abstract_artifact_generator import AbstractArtifactGenerator  # noqa
from .artifact_cache_singleton import ArtifactCacheSingleton  # noqa

"""

An Artifact is any result or output that is generated at some step in the process.
the initial onus of this design is to create room for a separate object which can encapsulate
various forms of QA throughout a data pipeline.

for example, maybe a data pipeline is setup to read weather data, once it is initially parsed and loaded
and artifact generator can be used to chart out the data into a matplotlib figure.


"""
