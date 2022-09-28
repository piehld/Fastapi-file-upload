"""
Run with `uvicorn main:app`

Then run `python client.py`
"""

from fastapi import FastAPI, Form, UploadFile, HTTPException
import glob
import os
import logging
from time import sleep

app = FastAPI()
log = logging.getLogger(__name__)
DATA_DIR = "data"

@app.get("/purge")
def purge():
    cwd = os.getcwd()
    files = glob.glob(f'{cwd}/{DATA_DIR}/*')
    for f in files:
        os.remove(f)
        
@app.post("/upload")
async def upload(file: UploadFile, 
    file_name : str = Form(...),
    chunk_index : int = Form(...),
    chunk_byte_offset: int = Form(...),
    total_chunks : int = Form(...),
    total_chunks_uploaded : int = Form(...),
    file_size : int = Form(...) ):

    save_path = os.path.join(DATA_DIR, file_name)
    if os.path.exists(save_path) and chunk_index == 0:
        log.error('File already exist. Remove it from the upload directory')
        raise HTTPException(status_code=500, detail="File already exists. ")
        
    try:
        with open(save_path, 'ab') as f:
            f.seek(chunk_byte_offset)
            f.write(file.file.read())
    except OSError:
        log.exception('Could not write to file')
        raise HTTPException(status_code=500, detail="Could not write to file")
    print("save_path size: ", os.path.getsize(save_path), "file_size", file_size)
    print("chunk index / total chunks", chunk_index, total_chunks)
    print("total_chunks_uploaded / total chunks", total_chunks_uploaded, total_chunks)
    if total_chunks_uploaded == total_chunks:
        if os.path.getsize(save_path) != file_size:
            print("BAD save_path size: ", os.path.getsize(save_path), "file_size", file_size)
            log.error(f"File {file_name} was completed, "
                      f"but has a size mismatch."
                      f"Was {os.path.getsize(save_path)} but we"
                      f" expected {file_size} ")
            raise HTTPException(status_code=500, detail="size mismatch")
        else:
            log.info(f'File {file_name} has been uploaded successfully')
    else:
        log.debug(f'Chunk {chunk_index + 1} of {total_chunks} '
                  f'for file {file_name} complete')
    return {"message":f"Chunk #{chunk_index} upload successful for {file_name} "}