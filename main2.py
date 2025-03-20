import main


links = main.get_urls(main.buttons, 0.05, 0.05, 'show')
print(links)
# modified fetch function with semaphore
import asyncio
from aiohttp import ClientSession

async def fetch(url, session):
    async with session.get(url) as response:
        delay = response.headers.get("DELAY")
        date = response.headers.get("DATE")
        print("{}:{} with delay {}".format(date, response.url, delay))
        return await response.content()


async def bound_fetch(sem, url, session):
    # Getter function with semaphore.
    async with sem:
        await fetch(url, session)


async def run(urls):
    tasks = []
    # create instance of Semaphore
    sem = asyncio.Semaphore(1000)

    # Create client session that will ensure we dont open new connection
    # per each request.
    async with ClientSession() as session:
        for l in links:
            # pass Semaphore and session to every GET request
            task = asyncio.ensure_future(bound_fetch(sem, l, session))
            tasks.append(task)

        responses = asyncio.gather(*tasks)
        await responses

number = links
print(asyncio.run(run(number)))