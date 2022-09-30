import concurrent.futures
import requests
import os
import io
import time
from datetime import datetime
from copy import deepcopy
import asyncio
import httpx
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

chunk_size = 1024 * 1024 * 8
DEBUG = False
UPLOAD_URL = 'http://127.0.0.1:8000/upload'
PURGE_URL = 'http://127.0.0.1:8000/purge'
FILES_TO_UPLOAD = [
    # '/Users/dennis/rcsb/py-rcsb_app_file/rcsb/app/tests-file/test-data/bigFile.txt',
    # '/Users/dennis/rcsb/py-rcsb_app_file/rcsb/app/tests-file/test-data/bigFile.txt.256mb',
    # '/Users/dennis/rcsb/py-rcsb_app_file/rcsb/app/tests-file/test-data/bigFile.txt.5gb',
    '/Users/dennis/rcsb/py-rcsb_app_file/rcsb/app/tests-file/test-data/bigFile.txt.14gb',
    '/Users/dennis/rcsb/py-rcsb_app_file/rcsb/app/tests-file/test-data/bigFile.txt.30gb',
]

def serial_test():
    print(f'Uploading {len(FILES_TO_UPLOAD)} files serially')
    results = []
    for file in FILES_TO_UPLOAD:
        results.append(upload_file(file))
    print("Serial Uploading Result")
    for result in results:
        print(result)

def concurrent_test():
    print(f'Uploading {len(FILES_TO_UPLOAD)} files with threadpool')
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # futures = {executor.submit(upload_file, file): file for file in FILES_TO_UPLOAD}
        futures = {executor.submit(asyncio.run, upload_file(file)): file for file in FILES_TO_UPLOAD}
        results = []
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
        print("Multi-threaded multiple files result")
        for result in results:
            print(result)

def human_friendly(bites):
    unit = "B"
    if bites > 1024:
        bites = bites/1024
        unit = 'KB'
    if bites > 1024:
        bites = bites/1024
        unit = 'MB'
    if bites > 1024:
        bites = bites/1024
        unit = 'GB'
    return f"{bites} {unit}"

def testing_stats(func):
    '''For calculating time for a given function'''
    def get_statistics(*args, **kwargs):
        timings = {}
        file_path = args[0]
        size = os.path.getsize(file_path)
        timings['File_size'] = human_friendly(size)
        start = time.time()
        r = func(*args, **kwargs)
        if isinstance(r, concurrent.futures.Future):
            r = r.result()
        if r.status_code == 200:
            timings['Duration'] = time.time() - start
            return timings
        else:
            print("No Stats. Request Failed")
            return None
    return get_statistics

def async_request(req_body, tmp):
    return requests.post(UPLOAD_URL, data=req_body, files={'file': tmp})

# @testing_stats
async def upload_file(file_path):
    response = None
    total_chunks = 0
    file_size = os.path.getsize(file_path)

    if chunk_size < file_size:
        total_chunks = file_size // chunk_size
        if file_size % chunk_size:
            total_chunks = total_chunks + 1
    else:
        total_chunks = 1

    req_body = {
        "file_name": file_path.split('/')[-1],
        "chunk_index": 0,
        "chunk_byte_offset": 0,
        "total_chunks": total_chunks,
        "file_size": file_size
    }

    # if ASYNC:
        # pool = concurrent.futures.ThreadPoolExecutor(max_workers=10)

    maxActiveTasks = 30
    maxThreads = 10
    tmp = io.BytesIO()
    async with httpx.AsyncClient(timeout=None) as client:
        with open(file_path, "rb") as to_upload:
            tasks = []
            for i in range(0, req_body["total_chunks"]):
                packet_size = min(req_body["file_size"] - (req_body["chunk_index"]*chunk_size), chunk_size)
                tmp.truncate(packet_size)
                tmp.seek(0)
                tmp.write(to_upload.read(packet_size))
                tmp.seek(0)
                if ASYNC:
                    # print(req_body)
                    # response = pool.submit(async_request, deepcopy(req_body), deepcopy(tmp))
                    filesD = {"file": deepcopy(tmp)}
                    tasks.append(asyncio.ensure_future(semaphoreTask(client, maxThreads, deepcopy(req_body), filesD)))
                else:
                    response = requests.post(UPLOAD_URL, data=req_body, files={'file': tmp})
                if DEBUG and not ASYNC:
                    print(str(response.content))
                if not ASYNC and response.status_code != 200:
                    break
                req_body["chunk_index"] = req_body["chunk_index"] + 1
                req_body["chunk_byte_offset"] = req_body["chunk_index"] * chunk_size
                try:
                    if len(tasks) == maxActiveTasks:
                        retList = await asyncio.gather(*tasks)  # NOTE: This runs the tasks and clears them from memory, and closes the file handles
                        # logger.info("Slice upload return list: %r", retList)
                        tasks = []
                except Exception as e:
                    logger.exception("Failing with %s", str(e))
            #
            try:
                if len(tasks) > 0:
                    retList = await asyncio.gather(*tasks)
                    # logger.info("Slice upload return list: %r", retList)
                    tasks = []
            except Exception as e:
                logger.exception("Failing with Exception %s", str(e))
    return response

async def semaphoreTask(client, maxThreads, mD, filesD):
    semaphore = asyncio.Semaphore(maxThreads)
    async with semaphore:
        ok = False
        response = await client.post(UPLOAD_URL, data=mD, files=filesD)
        if response.status_code == 200:
            ok = True
        else:
            logger.error("response %r %r", response.status_code, response.text)
        #
        if ok:
            return (mD["chunk_index"], ok)
        else:
            return False


if __name__ == "__main__":
    # print('Without ASYNC')
    # ASYNC = False
    # response = requests.get(PURGE_URL)
    # serial_test()  # Each file is uploaded one after another
    # response = requests.get(PURGE_URL)
    # concurrent_test()  # 10 files being uploaded concurrently

    print('With ASYNC')
    ASYNC = True  # Same file can have upto 10 chunks sent concurrently
    # response = requests.get(PURGE_URL)
    # serial_test()
    response = requests.get(PURGE_URL)
    startTime = time.time()
    print(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
    concurrent_test()
    print(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
    logger.info("Completed upload (%.4f seconds)", time.time() - startTime)
    # for f in FILES_TO_UPLOAD:
    #     startTime = time.time()
    #     print(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
    #     r = asyncio.run(upload_file(f))
    #     print("RESULT: ", r)
    #     print(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
    #     logger.info("Completed upload (%.4f seconds)", time.time() - startTime)
