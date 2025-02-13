from inspect import isasyncgen, iscoroutine, isgenerator
from pydantic import BaseModel
from typing import Union, List, Any


async def yield_data_batches(data: object):
    if iscoroutine(data):
        data = await data

    if isgenerator(data) or isasyncgen(data):
        if isasyncgen(data):
            async for d in data:
                yield d
        else:
            for d in data:
                yield d
    else:
        yield data


def get_python_objects_from_pydantic_model(
    model_data: Union[BaseModel, List[BaseModel]]
) -> Union[dict, List[dict]]:
    """
    Recursively converts a BaseModel or a list of BaseModel instances
    (including any nested BaseModels) into plain Python objects (dictionaries).
    """

    def _recursive_convert(item: Any) -> Any:
        if isinstance(item, BaseModel):
            # Use model_dump with mode='python' to preserve native types.
            dumped = item.model_dump(mode="python")
            return _recursive_convert(dumped)
        elif isinstance(item, dict):
            return {k: _recursive_convert(v) for k, v in item.items()}
        elif isinstance(item, list):
            return [_recursive_convert(elem) for elem in item]
        else:
            return item

    return _recursive_convert(model_data)
