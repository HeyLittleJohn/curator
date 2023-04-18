import asyncio

import aiohttp
from aiomultiprocess import Pool, set_start_method


async def fetch(session, url):
    try:
        async with session.get(url) as response:
            if response.status != 200:
                response.raise_for_status()
            return {"url": url, "action": "success", "status_code": response.status}
    except Exception:
        return {"url": url, "action": "error", "status_code": 404}


async def fetch_all(session, urls):
    async with Pool() as pool:
        starargs = [(session, url) for url in urls]
        results = await pool.starmap(fetch, starargs)
        return results


async def main():
    urls = ["www.google.com"] * 100
    async with aiohttp.ClientSession() as session:
        dataset = await fetch_all(session, urls)
    print(dataset)


if __name__ == "__main__":
    set_start_method("fork")
    asyncio.run(main())
