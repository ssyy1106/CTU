import asyncio

async def hello(i):
    print(f"hello {i} started")
    await asyncio.sleep(4)
    print(f"hello {i} done")

async def main():
    task1 = asyncio.create_task(hello(1))  # returns immediately, the task is created
    #await asyncio.sleep(3)
    task2 = asyncio.create_task(hello(2))
    await task1
    await task2

# asyncio.run(main())  # main loop
# asyncio.run(main())  # main loop
# print('end')

def gener():
    for i in range(10):
        print(f"begin {i}")
        yield i
        print(f"end {i}")

def coroutine():
    print('begin')
    x = yield
    print(f"end receive {x}")
    yield 2500

my_coro = coroutine()
print(my_coro)
print(next(my_coro))
print('after next')
res = my_coro.send(250)
print(res)
print('here')
print(next(my_coro))
print('exit')