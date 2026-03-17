from collections import namedtuple

Result = namedtuple('Result', 'count average')
# 子生成器
def averager():
    total = 0.0
    count = 0
    average = None
    while True:
        term = yield average
        if term is None: 
            break
        total += term
        count += 1
        average = total/count
        print(f"average: {average} total: {total} count: {count}")
    return Result(count, average) 
    
def grouper(results, key):
    #while True:
    print(f"Processing group: {key}")
    results[key] = yield from averager()
    print(f"Finished processing group: {key} results: {results[key]}")
    #yield 1

def main(data):
    results = {}
    for key, values in data.items():
        group = grouper(results, key)
        val = next(group)
        print(f"val: {val}")
        for value in values:
            group.send(value)
        try:
            group.send(None) # 重要！ ⓬  
        except StopIteration:
            pass
    #print(results) # 如果要调试，去掉注释
    report(results)

def report(results):
    for key, result in sorted(results.items()):
        group, unit = key.split(';')
        print('{:2} {:5} averaging {:.2f}{}'.format(
            result.count, group, result.average, unit))

data = {
    'girls;kg':
    [40.9, 38.5, 44.3, 42.2, 45.2, 41.7, 44.5, 38.0, 40.6, 44.5],
    'girls;m':
    [1.6, 1.51, 1.4, 1.3, 1.41, 1.39, 1.33, 1.46, 1.45, 1.43],
    'boys;kg':
    [39.0, 40.8, 43.2, 40.8, 43.1, 38.6, 41.4, 40.6, 36.3],
    'boys;m':
    [1.38, 1.5, 1.32, 1.25, 1.37, 1.48, 1.25, 1.49, 1.46],
}
if __name__ == '__main__':
    main(data)

