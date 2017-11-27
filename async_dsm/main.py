import asyncio

async def compute(x, y):
    print("Compute %s + %s ..." % (x, y))
    await asyncio.sleep(1.0)
    return x + y

async def print_sum(future, x, y):
    result = await compute(x, y)
    r2 = await future
    print("%s + %s = %s" % (x, y, result))

future = asyncio.Future()
future.set_exception(KeyError())

loop = asyncio.get_event_loop()
loop.run_until_complete(print_sum(future, 2, 4))
loop.close()