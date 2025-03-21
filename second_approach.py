import aiohttp
import asyncio
from connection_db import Connection_db



con = Connection_db('data.db')
urls = con.get_links()
dir_path = 'data3/'

async def download_file(url, filename):
  async with aiohttp.ClientSession() as session:
    async with session.get(url) as response:
      if response.status == 200:
        with open(filename, 'wb') as f:
          while True:
            chunk = await response.content.read()
            if not chunk:
              break
            f.write(chunk)
        print(f"Downloaded {filename}")
      else:
        print(f"Error downloading {filename}: {response.status}")

async def main():
 
  tasks = [aiohttp.create_task(download_file(url, filename)) for url, filename in urls]
  await asyncio.gather(*tasks)

asyncio.run(main())