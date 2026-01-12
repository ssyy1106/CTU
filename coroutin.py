import os
import time
import sys
import requests
from concurrent import futures

CC = ['CN', 'IN', 'US', 'ID', 'BR', 'PK', 'NG', 'BD', 'RU', 'JP', 'MX', 'PH', 'VN', 'ET', 'EG', 'DE', 'IR', 'TR', 'CD', 'FR']
BASE_URL = 'http://flupy.org/data/flags'
DEST_DIR = 'c:/downloads/'
MAX_WORKERS = 20

def save_flag(img, filename):
    path = os.path.join(DEST_DIR, filename)
    print(f"path: {path}")
    with open(path, 'wb') as fp:
        fp.write(img)

def get_flags(cc):
    url = f"{BASE_URL}/{cc.lower()}/{cc.lower()}.gif"
    resp = requests.get(url)
    print(f"url: {url}")
    return resp.content

def show(text):
    print(text, end=' ')
    sys.stdout.flush()

def download_one(cc):
    img = get_flags(cc)
    show(cc)
    save_flag(img, cc.lower() + '.gif')
    return cc

def download(CC):
    for cc in CC:
        download_one(cc)

def download_currency(CC):
    # executor = futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)
    # executor.map(download_one, CC)
    with futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # for cc in CC:
        #     executor.submit(download_one, cc)
        executor.map(download_one, CC)

def download_many(CC):
    # CC = CC[:5]
    with futures.ProcessPoolExecutor(max_workers=20) as executor:
        to_do = []
        for cc in CC:
            future = executor.submit(download_one, cc)
            to_do.append(future)
            print(f"scheduled for {cc}: {future}")
        res = []
        for future in futures.as_completed(to_do):
            result = future.result()
            print(f"{future} result: {result}")
            res.append(result)


def main(getFlags):
    start = time.time()
    getFlags(CC)
    elapsed = time.time() - start
    print(f"flags downloaded in {elapsed}s")

if __name__ == "__main__":
    main(download_currency)
    print('done')