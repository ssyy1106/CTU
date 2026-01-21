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
    print('end coroutine')

def average():
    total = 0
    count = 0
    x = yield
    try:
        while True:
            if x is not None:
                total += x
                count += 1
                x = yield total / count
            else:
                x = yield None
    except GeneratorExit:
        print("Average coroutine closed.")
        yield 888

my_aver = average()
print(next(my_aver))
print(my_aver.send(100))
print(my_aver.send(200))
print(my_aver.send(None))
print(my_aver.send(100))
print(my_aver.send(800))
print(my_aver.close())

# my_coro = coroutine()
# print(my_coro)
# print(next(my_coro))
# print('after next')
# res = my_coro.send(250)
# print(res)
# print('here')
# print(next(my_coro))
# print('exit')

# gen = gener()
# print(next(gen))
# print('after next')
# print(next(gen))
# print('here')
# print(next(gen))