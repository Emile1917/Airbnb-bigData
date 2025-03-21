import urllib.parse

# modified fetch function with semaphore

import asyncio
import aiohttp
import urllib
import math
import os
from tenacity import retry, stop_after_attempt, wait_exponential
from connection_db import Connection_db
from main import WebScraper
import pandas as pd
import time
import datetime
import hashlib


conn = Connection_db('data.db')
#scraper = WebScraper(conn)
links = conn.get_links()
dir_path = 'data'
MB_unit = math.pow(2, 20)
df = pd.DataFrame(columns=['url','size','last-modified','hash'])
df['url'] = links
metadata_dir = 'metadata'

# Asynchronous function to download a file with timeout handling and retries
@retry(stop=stop_after_attempt(20), wait=wait_exponential(multiplier=1, min=1, max=20))
async def download_file(session, url, semaphore):
    async with semaphore:  # Limit concurrent downloads
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10000)) as response:
                if response.status == 200:
                    df.loc[df['url'] == url, 'size'] = (float(response.headers['Content-Length']) / MB_unit)
                    df.loc[df['url'] == url, 'last-modified'] = response.headers['Last-Modified'] if response.headers['Last-Modified'] else time.strftime('%Y-%m-%d %H:%M:%S')
                    return await response.read()
                else:
                    print(
                        f"Failed to download {url}: Status code {response.status}")
                    return None
        except asyncio.TimeoutError:
            print(f"Timeout occurred while downloading {url}")
            raise  # Raise the exception to trigger a retry
        except Exception as e:
            print(f"An error occurred while downloading {url}: {e}")
            raise  # Raise the exception to trigger a retry

# Main function to manage the download tasks


async def main(urls):
    semaphore = asyncio.Semaphore(10000)  # Limit to 10 concurrent downloads
    async with aiohttp.ClientSession() as session:
        tasks = [download_file(session, url, semaphore) for url in urls]
        results = await asyncio.gather(*tasks)
        return results


def chunks(lst, offset):
    if len(lst) > offset:
        for i in range(math.ceil(len(links)/offset)):
            yield lst[i*offset:(i+1)*offset]
    else:
        yield lst

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

offset = 1000
urls_array = list(chunks(links, offset))
# Running the main function

if __name__ == '__main__':
    for urls in urls_array:
        results = asyncio.run(main(urls))
        for url, data in zip(urls, results):
            if data:
                
                url_processed = process_url(url)
                domain = make_dir(str(dir_path)+url_processed['domain'])
                file = f"{url_processed['country']}-{url_processed['state']}-{url_processed['city']}-{url_processed['date']}-{url_processed['type']}-{url_processed['file']}"
                h1 = hashlib.new('sha256')
                h1.update(data)
                df.loc[df['url'] == url, 'hash'] = h1.hexdigest()
                with open(os.path.join(domain,file) , 'wb') as f:
                    f.write(data)
                print(f"Downloaded {url} successfully.")
            else:
                print(f"Failed to download {url}.")
    print(df)
    df.to_parquet(f"{metadata_dir}/meta.parquet")

