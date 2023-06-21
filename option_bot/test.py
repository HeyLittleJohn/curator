import asyncio

import aiohttp
from aiomultiprocess import Pool


async def fetch(url):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    response.raise_for_status()
                return {"url": url, "action": "success", "status_code": response.status}
        except Exception as e:
            print(e)
            return  # {"url": url, "action": "error", "status_code": 404}


async def fetch_all(urls):
    async with Pool(processes=1, childconcurrency=1) as pool:
        results = await pool.map(fetch, urls)
        return results


async def main():
    urls = ["https://www.google.com"] * 100
    #
    dataset = await fetch_all(urls)
    print(dataset)


if __name__ == "__main__":
    asyncio.run(main())
