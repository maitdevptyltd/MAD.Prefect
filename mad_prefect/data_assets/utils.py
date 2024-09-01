from inspect import isasyncgen, iscoroutine, isgenerator


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
