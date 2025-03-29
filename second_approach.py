import aiohttp
import asyncio
from connection_db import Connection_db
import urllib
import os
from tenacity import retry, stop_after_attempt, wait_exponential
import pandas as pd
import math

con = Connection_db('data.db')
urls = con.get_links()
dir_path = 'data3/'
timeout = aiohttp.ClientTimeout(total=10000)

def process_url(url: str) -> dict:
    url_value = str(url).split('//', 2)[1].split('/')
    domain = url_value[0]
    country = urllib.parse.unquote(url_value[1])
    state = urllib.parse.unquote(url_value[2]) if len(url_value) == 7 else url[1] if len(url_value) == 5 else ''
    city = urllib.parse.unquote(url_value[3]) if len(url_value) == 7 else url[1] if len(url_value) == 5 else ''
    date = urllib.parse.unquote(url_value[4]) if len(url_value) == 7 else url[1] if len(url_value) == 5 else ''
    type = urllib.parse.unquote(url_value[-2])
    file = urllib.parse.unquote(url_value[-1])
    return {
        'domain': domain,
        'country': country,
        'state': state,
        'city': city,
        'date': date,
        'type': type,
        'file': file
    }


def make_dir(path: str) -> str:
    if not os.path.isdir(path):
        os.makedirs(path)
    return path



@retry(stop=stop_after_attempt(1), wait=wait_exponential(multiplier=1, min=1, max=1))
async def download_file(url, filename):
  semaphore = asyncio.Semaphore(10000)
  async with semaphore:
    try:
      async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as response:
          if response.status == 200:
            domain = make_dir(str(dir_path)+process_url(url)['domain'])
            with open(domain+'/'+filename, 'wb') as f:
              while True:
                chunk = await response.content.read()
                if not chunk:
                  break
                f.write(chunk)
            print(f"Downloaded {filename}")
          else:
            print(f"Error downloading {filename}: {response.status}")
    except asyncio.TimeoutError:
            print(f"Timeout occurred while downloading {url}")
            raise  # Raise the exception to trigger a retry
    except Exception as e:
            print(f"An error occurred while downloading {url}: {e}")
            with open('errors.txt', 'wb') as f:
              f.write(url) 
            raise  # Raise the exception to trigger a retry
async def main(links): 
  tasks = [(download_file(url, f"{process_url(url)['country']}-{process_url(url)['state']}-{process_url(url)['city']}-{process_url(url)['date']}-{process_url(url)['type']}-{process_url(url)['file']}")) for url in links]
  results = await asyncio.gather(*tasks)
  return results

size = 500
partition = math.ceil(len(urls)/size)
links = [ urls[i * size : (i+1 * size)] for i in range(partition)]
for l in links :
  asyncio.run(main(l))