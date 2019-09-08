import asyncio
import json
from multiprocessing import cpu_count, Pool, Process
from functools import partial
from time import time
import pandas as pd
import math
import pymssql
import requests
import shutil
import os
import logging

logging.basicConfig(filename='download_image_v2_with_error_log.log', level=logging.INFO, filemode='w', format='%(name)s - %(levelname)s - %(message)s')

PATH = "image_error_1st/"

SQL_HOST='172.20.2.110'
SQL_USER='gpkt'
SQL_PWD='Paytv@gpkt~!@#'
SQL_DB='RecommendDB'
STORE="IN_Metadata_Person_GetlistImage"

sqlserver = pymssql.connect (
        server=SQL_HOST,
        user=SQL_USER,
        password=SQL_PWD,
        database=SQL_DB,
        as_dict=True,
        charset="UTF-8",
    )

async def get_metadata_from_mssql(store, *args, **kwargs):
    cur = sqlserver.cursor()
    cur.execute("exec %s"%(store))
    result = cur.fetchall()
    
    return result

async def get_metadata_from_log_error(file_log, *args, **kwargs):
    result = []

    with open(file_log, "r") as input_file:
        
        line = input_file.readline()

        while line:
            if "INFO" in line:
                try:
                    term = line.replace("root - INFO - ", "")
                    result.append(json.loads(term))
                except Exception as e:
                    print(e)
                    print(line)
                    break
            line = input_file.readline()

    return result

def save_image(info):
    try: 

        img_url = info["Headshot"]
        image_name = "%s.%s"%(info["ID"], img_url.split('.')[-1])
        with open(PATH+image_name, 'wb') as output_file,\
            requests.get(img_url, stream=True, timeout=5) as response:
            shutil.copyfileobj(response.raw, output_file)
    
    except Exception as e:
        logging.error(e)
        logging.info(json.dumps(info))

async def main(*args, **kwargs):

    start = time()

    if "use_log_file" in kwargs:
        result = await get_metadata_from_log_error("download_image_v2.log")
    
    else: 
        result = await get_metadata_from_mssql(STORE)

    pool = Pool(processes=cpu_count()*2)
    result = pool.map(func=save_image, iterable=result)

    print("time_ex:", time()-start)
    
if __name__ == '__main__':
    # asyncio.get_event_loop().run_until_complete(main())
    asyncio.get_event_loop().run_until_complete(main(use_log_file = True))