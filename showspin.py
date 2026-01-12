import threading
import itertools
import time
import sys
import asyncio

class Signal:
    go = True

def spin(message, signal):
    write, flush = sys.stdout.write, sys.stdout.flush
    for char in itertools.cycle('|/-\\'):
        status = char + ' ' + message
        write(status)
        flush()
        write('\x08' * len(status))
        time.sleep(.1)
        if not signal.go:
            break
    write(' ' * len(status) + '\x08' * len(status))

def slow_function():
    time.sleep(3)
    return 42

@asyncio.coroutine
def supervisor():
    spinner = asyncio.run(spin('thinking!'))
    result = yield from slow_function()
    spinner
    #signal = Signal()
    # spinner = threading.Thread(target=spin, args = ('thinking!', signal))
    # print('spinner object:', spinner)
    # spinner.start()
    # result = slow_function()
    # signal.go = False
    # spinner.join()
    # return result

def main():
    #result = supervisor()
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(supervisor())
    loop.close()
    print('Answer: ', result)

if __name__ == '__main__':
    main()
