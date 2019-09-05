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

logging.basicConfig(filename='download_image.log', level=logging.INFO, filemode='w', format='%(name)s - %(levelname)s - %(message)s')

PATH = "image_new/"

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

def save_image(info):
    try: 

        img_url = info["Headshot"]
        image_name = "%s.%s"%(info["ID"], img_url.split('.')[-1])
        with open(PATH+image_name, 'wb') as output_file,\
            requests.get(img_url, stream=True, timeout=3) as response:
            shutil.copyfileobj(response.raw, output_file)
    
    except Exception as e:
        logging.error(e)
        logging.info(json.dumps(info))

def save_image_2(_id, _name, _headshot):
    try: 

        info = {
            "ID": _id,
            "Headshot": _headshot,
            "Name": _name
        }
        # print(info)
        save_image(info)
    
    except Exception as e:
        print(e)

async def task_download_image(result, i):
    processes = []
    for rs in result:
        p = Process(target=save_image_2, args=(rs["ID"], rs["Name"], rs["Headshot"]))
        processes.append(p)
        p.start()

    for p in processes:
            p.join()

    print(i)

async def main():

    start = time()

    
    result = await get_metadata_from_mssql(STORE)


    for i in range(0, len(result), 800):
        
        await task_download_image(result[i: i+800], i)

    print("time_ex:", time()-start)
if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())