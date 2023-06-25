from bs4 import BeautifulSoup
from pythonjsonlogger import jsonlogger
from aiolimiter import AsyncLimiter
from urllib.parse import urlparse
import asyncio
import aiohttp
import logging
import time
import random
import aiofiles
import json

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)

def parse_html(html):
    soup = BeautifulSoup(html, 'html.parser')

    # Find the elements containing the data you want to scrape
    asking_price_element = soup.find('span', string='Asking Price:')
    asking_price = asking_price_element.find_next_sibling('b').text.strip() if asking_price_element else 'N/A'

    cash_flow_element = soup.find('span', string='Cash Flow:')
    cash_flow = cash_flow_element.find_next_sibling('b').text.strip() if cash_flow_element else 'N/A'

    gross_revenue_element = soup.find('span', string='Gross Revenue:')
    gross_revenue = gross_revenue_element.find_next_sibling('b').text.strip() if gross_revenue_element else 'N/A'

    # need to do for the rest of the data points.

    # Return the data as a dictionary
    return {
        'Asking Price': asking_price,
        'Cash Flow': cash_flow,
        'Gross Revenue': gross_revenue,
        # add on for the rest of the data points.
    }

async def HTTPClientDownloader(url, settings):
    host = urlparse(url).hostname
    max_tcp_connections = settings['max_tcp_connections']

    async with settings['rate_per_host'][host]["limit"]:
        connector = aiohttp.TCPConnector(limit=max_tcp_connections)

        async with aiohttp.ClientSession(connector=connector) as session:
            start_time = time.perf_counter()  # Start timer
            safari_agents = [
                'Safari/17612.3.14.1.6 CFNetwork/1327.0.4 Darwin/21.2.0',  # works!
            ]
            user_agent = random.choice(safari_agents)

            headers = {
                'User-Agent': user_agent
            }

            proxy = None
            html = None
            async with session.get(url, proxy=proxy, headers=headers) as response:
                html = await response.text()
                data = parse_html(html)  # Parse the HTML and extract the data
                end_time = time.perf_counter()  # Stop timer
                elapsed_time = end_time - start_time  # Calculate time taken to get response
                status = response.status

                logger.info(
                    msg=f"status={status}, url={url}",
                    extra={
                        "elapsed_time": f"{elapsed_time:4f}",
                    }
                )

                dir = "./data"
                idx = url.split(
                    "https://www.bizbuysell.com/new-york-businesses-for-sale/")[-1]
                loc = f"{dir}/bizbuysell-ny-{idx}.json"

                async with aiofiles.open(loc, mode="w") as fd:
                    await fd.write(json.dumps(data))  # Save the data as JSON

async def dispatch(url, settings):
    await HTTPClientDownloader(url, settings)

async def main(start_urls, settings):
    tasks = []
    for url in start_urls:
        task = asyncio.create_task(dispatch(url, settings))
        tasks.append(task)

    results = await asyncio.gather(*tasks)
    print(f"total requests", len(results))

if __name__ == '__main__':
    settings = {
        "max_tcp_connections": 1,
        "proxies": [
            "http://localhost:8765",
        ],
        "rate_per_host": {
            'www.bizbuysell.com': {
                "limit": AsyncLimiter(10, 60),  # 10 requests per minute
            },
        }
    }
    
    start_urls = []
    start, end = 1, 13  # For demo purpose
    for i in range(start, end):
        url = f"https://www.bizbuysell.com/new-york-businesses-for-sale/{i}"
        start_urls.append(url)
    
    asyncio.run(main(start_urls, settings))