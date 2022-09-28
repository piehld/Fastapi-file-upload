import concurrent.futures
import requests
import os
import io
from datetime import datetime
import time
import httpx
import asyncio
import aiohttp

chunk_size = 1024  * 1024 * 8
DEBUG = False
UPLOAD_URL='http://127.0.0.1:8000/upload'
PURGE_URL='http://127.0.0.1:8000/purge'
FILES_TO_UPLOAD = [
    '/Users/dennis/rcsb/py-rcsb_app_file/rcsb/app/tests-file/test-data/bigFile.txt.256mb',
]

def serial_test():
    print(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
    print(f'Uploading {len(FILES_TO_UPLOAD)} files serially')
    results = []
    for file in FILES_TO_UPLOAD:
        results.append(upload_file(file))
    print("Serial Uploading Result")
    for result in results:
        print(result)
    print(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

def concurrent_test():
        print(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
        print(f'Uploading {len(FILES_TO_UPLOAD)} files with threadpool')
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            futures = {executor.submit(upload_file, file): file for file in FILES_TO_UPLOAD}
            # results = []
            # for future in concurrent.futures.as_completed(futures):
            #     results.append(future.result())
            # print("Multi-threaded multiple files result")
            # for result in results:
                # print(result)
        print(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

def human_friendly(bites):
    unit = "B"
    if bites >1024:
        bites = bites/1024
        unit ='KB'
    if bites>1024:
        bites = bites/1024
        unit = 'MB'
    if bites>1024:
        bites = bites/1024
        unit = 'GB'
    return f"{bites} {unit}"

def testing_stats(func):
    '''For calculating time for a given function'''
    def get_statistics(*args, **kwargs):
        timings={}
        file_path = args[0]
        size = os.path.getsize(file_path)
        timings['File_size'] = human_friendly(size)
        start = time.time()
        r = func(*args, **kwargs)
        if isinstance(r,concurrent.futures.Future):
            r = r.result()
        if r.status_code == 200:
            timings['Duration'] = time.time() - start
            return timings
        else:
            print("No Stats. Request Failed")
            return None
    return get_statistics

async def gather_async_request(file_path, req_body):
    tL = []
    tmp =  io.BytesIO()
    with requests.Session() as client:
        with open(file_path, "rb") as to_upload:
            for i in range(0, req_body["total_chunks"]):
                req_body["total_chunks_uploaded"]+=1
                packet_size =min(req_body["file_size"] - (i*chunk_size), chunk_size)
                req_body["chunk_index"] = i
                req_body["chunk_byte_offset"] = req_body["chunk_index"] * chunk_size
                tmp.truncate(packet_size)
                tmp.seek(0)
                tmp.write(to_upload.read(packet_size))
                tmp.seek(0)
                tL.append(client.post(UPLOAD_URL,data=req_body, files={'file' : tmp}))
        # res = await asyncio.gather(*tL)
        # print("res", res)
        # return res
        return await asyncio.gather(*tL)

@testing_stats
def upload_file(file_path):
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
        "file_name" : file_path.split('/')[-1],
        "chunk_index" : 0,
        "chunk_byte_offset" : 0,
        "total_chunks" : total_chunks,
        "total_chunks_uploaded" : 0,
        "file_size" : file_size
    }

    if ASYNC:
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)  # FASTER WITH "1" instead of "10"?! That pointed out, true "async" should occur only on one thread...

    tmp =  io.BytesIO()

    tot= 0
    total_chunks_uploaded =0
    response = asyncio.run(gather_async_request(file_path, req_body))
    # return response
    return



if __name__ == "__main__":
    # print('Without ASYNC')
    # ASYNC=False 
    # response = requests.get(PURGE_URL)
    # serial_test() # Each file is uploaded one after another
    # response = requests.get(PURGE_URL) 
    # concurrent_test() # 10 files being uploaded concurrently

    print('With ASYNC')
    ASYNC=True # Same file can have upto 10 chunks sent concurrently
    # response = requests.get(PURGE_URL)
    # serial_test()
    response = requests.get(PURGE_URL)
    concurrent_test()