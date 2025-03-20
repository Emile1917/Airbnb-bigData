import requests
import os
import time

import aiohttp
import asyncio
from aiohttp import ClientSession
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

# Set up the WebDriver (make sure to specify the correct path to your WebDriver)
driver = webdriver.Chrome()  # or webdriver.Firefox(), etc.
waiterdriver = WebDriverWait(driver, 10)
# Open the desired web page
# Replace with the URL of the page you want to scrape
url = 'https://insideairbnb.com/fr/get-the-data/'
driver.get(url)

# Find all tables on the page
'''
buttons = waiterdriver.find_elements(By.CLASS_NAME,'showArchivedData')

tables = waiterdriver.find_elements(By.TAG_NAME, 'table')'
'''''
# button = waiterdriver.until(ec.element_to_be_clickable((By.CLASS_NAME,'showArchivedData')))
buttons = driver.find_elements(By.CLASS_NAME, 'showArchivedData')
buttons = buttons
x_path = '//td//a'

'''
for button in buttons:
    # Scroll to the element
    time.sleep(0.0001)
    driver.execute_script("arguments[0].scrollIntoView();", button)
    driver.execute_script("arguments[0].click();", button)
    # button.click()
    time.sleep(0.0001)
# button.click()
# Wait for a few seconds to see the effect
try:
    elements = driver.find_elements(By.LINK_TEXT, 'show')
    print('There is still '+str(len(elements))+' elements')
    if len(elements) > 0:
        for e in elements:
            driver.execute_script("arguments[0].scrollIntoView();", e)
            driver.execute_script("arguments[0].click();", e)
except Exception as e:
    print(e)
element_links = driver.find_elements(By.XPATH, x_path)
print(len(element_links))
links = list(map(lambda x: x.get_attribute('href'), element_links))
# print(links)
time.sleep(0.5)
'''

# get_urls(elements, 0.5, 0.5, 'show')

def get_urls(dom_elements, delays, last_delay, dom_word):
    links = []
    for e in dom_elements[:5]:
        time.sleep(delays)
        driver.execute_script("arguments[0].scrollIntoView();", e)
        driver.execute_script("arguments[0].click();", e)
        time.sleep(delays)
    try:
      elements = driver.find_elements(By.LINK_TEXT, dom_word)
      if len(elements)  < 5:
          print('There is still '+str(len(elements))+' elements')
          get_urls(elements, delays, last_delay, dom_word)

      else:
          print('No more elements')
          element_links = driver.find_elements(By.XPATH, x_path)
          print(len(element_links))
          links = links + list(
                    map(lambda x: x.get_attribute('href'), element_links)
            )
          time.sleep(last_delay)
          return links      
    except Exception as e:
            print(e)
    



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

links = get_urls(buttons, 1, 1, 'show')
print(links)




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

async def fetch_with_pipelining():
    async with aiohttp.ClientSession() as session:
        urls = links
        async with session.get(urls[0]) as response:
            # Trigger pipelining by sending all requests before reading responses
            tasks = [session.get(url) for url in urls[1:]]
            responses = await asyncio.gather(response.read(), *tasks)
        for i, response in enumerate(responses):
            if isinstance(response, aiohttp.ClientResponse.read()):
                data = await response.read()
            else:
                data = response
            print(f"Request {i + 1}: {data}")
async def main():
    await fetch_with_pipelining()
asyncio.run(main())
