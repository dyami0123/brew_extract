# TODO:
# class CheckpointedPipelineBuilder(PipelineBuilder[T, S]):

#     def __init__(self, stacks: dict[str, TransformerStack[T, S]]):
#         super().__init__(stacks=stacks)

#     def add_checkpoint(self, stack_id: str) -> None:
#         checkpoint_transformer = CheckpointTransformer[T, S](pipeline_id=self.id, stack_id=stack_id)
#         self.add_transformer(checkpoint_transformer, target_stack_id=stack_id)

#     def generate(self) -> dict[str, T]:
#         # find most recent checkpoint for each stack

#     def _generate_stack(self, stack_id: str) -> T:

#         reversed_stack = list(reversed(self.stacks[stack_id].transformers))

#         stack_append = []

#         for transformer in reversed_stack:
#             stack_append.append(transformer)
#             if isinstance(transformer, CheckpointTransformer):
#                 if transformer.checkpoint_exists():
#                     break

#         stack_append = list(reversed(stack_append))

#         first_transformer = stack_append[0]
#         if len(stack_append) == 1:
#             return first_transformer.transform(first_transformer.source.generate())


# def generate(self) -> dict[str, T]:
#     results = {stack_id: stack.generate() for stack_id, stack in self.stacks.items()}
#     self.artifact_cache.cache("results", "results", results)
#     return results

# def schemas(self) -> dict[str, S]:
#     schemas = {stack_id: stack.schema for stack_id, stack in self.stacks.items()}
#     self.artifact_cache.cache("schemas", "schemas", schemas)
#     return schemas
