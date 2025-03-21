import requests
import os
import time

import aiohttp
import asyncio
from aiohttp import ClientSession
import math
from tenacity import retry, stop_after_attempt, wait_exponential
import duckdb
'''
query = requests.get('https://data.insideairbnb.com/united-states/ny/albany/2025-03-02/data/listings.csv.gz')

if query.status_code == 200:
    print('Success')
    print(query.headers)
    raw_data = query.content
    with open('data/listings.csv.gz', 'wb') as f:
        f.write(raw_data)
else:
    print('Failed')
'''

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from connection_db import Connection_db


class WebScraper:
    def __init__(self,conn: Connection_db):
        self.conn = conn
        self.driver = webdriver.Chrome()  # or webdriver.Firefox(), etc.
        self.waiterdriver = WebDriverWait(self.driver, 10)
        self.url = 'https://insideairbnb.com/fr/get-the-data/'
        self.driver.get(self.url)
        self.buttons = self.driver.find_elements(By.CLASS_NAME, 'showArchivedData')
        self.x_path = '//td//a'
        
    def persist_data(self, data):
        present_links = set(self.conn.get_links())
        links_to_be_inserted = set(data)
        links_to_insert = links_to_be_inserted.difference(present_links)
        if len(links_to_insert) > 0:
            print('Links to be inserted: '+str(len(links_to_insert)))
            self.conn.insert_links('data', links_to_insert)
        else:
            print('No links to be inserted')

    def get_urls(self,dom_elements, delays, last_delay, dom_word):
        links = []
        for e in dom_elements:
            time.sleep(delays)
            self.driver.execute_script("arguments[0].scrollIntoView();", e)
            self.driver.execute_script("arguments[0].click();", e)
            time.sleep(delays)
        try:
            elements = self.driver.find_elements(By.LINK_TEXT, dom_word)
            if len(elements) > 0:
                print('There is still '+str(len(elements))+' elements')
                self.get_urls(elements, delays, last_delay, dom_word)

            else:
                print('No more elements')
                element_links = self.driver.find_elements(By.XPATH, self.x_path)
                print(len(element_links))
                links = links + list(
                            map(lambda x: x.get_attribute('href'), element_links)
                   )
                self.persist_data(links)

                # Create a BeautifulSoup object with the extracted HTML
                # Find all links in the BeautifulSoup object
                time.sleep(last_delay)
                return links
                
                
                #print(self.driver.get_downloadable_files())
                     
        except Exception as e:
                print(e)
                return links



'''
try:
    # Wait until the elements are present
    elements = WebDriverWait(driver, 10).until(
        ec.presence_of_all_elements_located((By.LINK_TEXT, 'Donate!'))  # Change to your locator
    )

    # Iterate through the list of elements and click each one if it's clickable
    for element in elements:
        try:
           
            print(element.get_attribute('outerHTML'))
            # Wait until the current element is clickable
            WebDriverWait(driver, 10).until(ec.element_to_be_clickable(element))
            element.click()
            # Optionally, you can add a small delay here if needed
        except Exception as e:
            print(f"An error occurred while clicking the element: {e}")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Close the driver
    driver.quit()
driver.quit()
'''

#links = get_urls(buttons, 1, 1, 'show')
#print(links)




'''
async def fetch(url, session):
    async with session.get(url,timeout=aiohttp.ClientTimeout(total=10000)) as response:
        delay = response.headers.get("DELAY")
        date = response.headers.get("DATE")
        print("{}:{} with delay {}".format(date, response.url, delay))
        print(response.read())
        return await response.read()


async def bound_fetch(sem, url, session):
    # Getter function with semaphore.
    async with sem:
        await fetch(url, session)


async def run(urls):
    tasks = []
    # create instance of Semaphore
    sem = asyncio.Semaphore(10000)

    # Create client session that will ensure we dont open new connection
    # per each request.
    async with ClientSession() as session:
        for l in urls:
            # pass Semaphore and session to every GET request
            task = asyncio.ensure_future(bound_fetch(sem, l, session))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
        for i, response in enumerate(responses):
            if isinstance(response, aiohttp.ClientResponse):
                data = await response
            else:
                data = response
            print(f"Response {i}: {data}")

number = links[:100]
data = asyncio.run(run(number))
print(data)'
'''


'''
async def fetch_with_pipelining():
    async with aiohttp.ClientSession() as session:
        urls = links[:5]
        async with session.get(urls[0]) as response:
            # Trigger pipelining by sending all requests before reading responses
            tasks = [session.get(url) for url in urls[1:]]
            responses = await asyncio.gather(response.read(), *tasks)
        for i, response in enumerate(responses):
            if isinstance(response, aiohttp.ClientResponse):
                data = await response.read()
            else:
                data = response
                with open('data/'+str(i)+'.csv.gz', 'wb') as f:
                    f.write(data)
            print(f"Request {i + 1}: {data}")
async def main():
    await fetch_with_pipelining()
asyncio.run(main())
'''

'''
import aiohttp
import asyncio

# Asynchronous function to download a file
async def download_file(session, url):
    async with session.get(url,timeout=aiohttp.ClientTimeout(total=10000)) as response:
        if response.status == 200:
            return await response.read()
        else:
            return None

# Main function to manage the download tasks
async def main(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [download_file(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        return results

# List of URLs to download
urls = links

# Running the main function
if __name__ == '__main__':
    results = asyncio.run(main(urls))
    for url, data in zip(urls, results):
        if data:
            with open('data/'+str(str(url).split('//',2)[1].replace('/','-')), 'wb') as f:
                f.write(data)
            print(f"Downloaded {url} successfully.")
        else:
            print(f"Failed to download {url}.")

'''


'''
# Asynchronous function to download a file with timeout handling and retries
@retry(stop=stop_after_attempt(20), wait=wait_exponential(multiplier=1, min=1, max=20))
async def download_file(session, url, semaphore):
    async with semaphore:  # Limit concurrent downloads
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10000)) as response:
                if response.status == 200:
                    return await response.read()
                else:
                    print(f"Failed to download {url}: Status code {response.status}")
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

# List of URLs to download
offset = 1000
part = math.ceil(len(links)/offset)
#urls = links

def chunks(lst,offset):
    for i in range(math.ceil(len(links)/offset)):
        yield lst[i*offset:(i+1)*offset]

urls_array = list(chunks(links,offset))
# Running the main function
if __name__ == '__main__':
    for urls in urls_array:
        results = asyncio.run(main(urls))
        for url, data in zip(urls, results):
            if data:
                with open('data/'+str(str(url).split('//',2)[1].replace('/','-')), 'wb') as f:
                    f.write(data)
                print(f"Downloaded {url} successfully.")
            else:
                print(f"Failed to download {url}.")'
'''