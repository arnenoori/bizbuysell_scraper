import csv
import os
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
import schedule
import time
import nest_asyncio
nest_asyncio.apply()


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)

# Create 'data' directory if it doesn't exist
if not os.path.exists('data'):
    os.makedirs('data')

def parse_listing_urls(html, url):
    try:
        soup = BeautifulSoup(html, 'html.parser')
        listing_elements = soup.find_all('a', class_='diamond')
        listing_urls = ['https://www.bizbuysell.com' + element['href'] for element in listing_elements]
        return listing_urls
    except Exception as e:
        logger.error(f"Error in parse_listing_urls: {str(e)}")
        return []
    
    
def parse_html(html, url):
    soup = BeautifulSoup(html, 'html.parser')

    title_element = soup.find('h1', class_='bfsTitle')
    title = title_element.text.strip() if title_element else 'N/A'
    
    location_element = soup.find('h2', class_='gray')
    location = location_element.text.strip() if location_element else 'N/A'

    asking_price_element = soup.find('span', string='Asking Price:')
    asking_price = asking_price_element.find_next_sibling('b').text.strip() if asking_price_element else 'N/A'

    cash_flow_element = soup.find('span', string='Cash Flow:')
    cash_flow = cash_flow_element.find_next_sibling('b').text.strip() if cash_flow_element else 'N/A'

    gross_revenue_element = soup.find('span', string='Gross Revenue:')
    gross_revenue = gross_revenue_element.find_next_sibling('b').text.strip() if gross_revenue_element else 'N/A'

    ebitda_element = soup.find('span', string='EBITDA:')
    ebitda = ebitda_element.find_next_sibling('b').text.strip() if ebitda_element else 'N/A'

    established_element = soup.find('span', string='Established:')
    established = established_element.find_next_sibling('b').text.strip() if established_element else 'N/A'

    inventory_element = soup.find('span', string='Inventory:')
    inventory = inventory_element.find_next_sibling('b').text.strip() if inventory_element else 'N/A'

    ffe_element = soup.find('span', string='FF&E:')
    ffe = ffe_element.find_next_sibling('b').text.strip() if ffe_element else 'N/A'

    business_description_element = soup.find('div', class_='businessDescription')
    business_description = business_description_element.text.strip() if business_description_element else 'N/A'

    detailed_info_elements = soup.find_all('dt')
    detailed_info = {}
    for element in detailed_info_elements:
        strong_element = element.find('strong')
        if strong_element is not None:
            key = strong_element.text.strip().rstrip(":")
            value = element.find_next_sibling('dd').text.strip()
            detailed_info[key] = value


    return {
        'Title': title,
        'Location': location,
        'Asking Price': asking_price,
        'Cash Flow': cash_flow,
        'Gross Revenue': gross_revenue,
        'EBITDA': ebitda,
        'Established': established,
        'Inventory': inventory,
        'FF&E': ffe,
        'Business Description': business_description,
        'URL': url,  # Add the URL to the dictionary
        **detailed_info,
    }


async def HTTPClientDownloader(url, settings):
    # Before scraping, check if we've already scraped this URL
    with open('scraped_urls.txt', 'a+') as f:
        f.seek(0)
        scraped_urls = f.read().splitlines()
    if url in scraped_urls:
        return

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
                listing_urls = parse_listing_urls(html, url)  # Get individual listing URLs
                
                for listing_url in listing_urls:
                    # Check if we've already scraped this URL
                    with open('scraped_urls.txt', 'r') as f:
                        scraped_urls = f.read().splitlines()
                    if listing_url in scraped_urls:
                        break

                    # Adding randomness to the rate limit
                    await asyncio.sleep(random.uniform(1, 5))
                                    
                    async with session.get(listing_url, proxy=proxy, headers=headers) as listing_response:
                        listing_html = await listing_response.text()
                        data = parse_html(listing_html, listing_url)  # Parse the HTML and include the URL
                        print(data)  # Print scraped data

                        end_time = time.perf_counter()  # Stop timer
                        elapsed_time = end_time - start_time  # Calculate time taken to get response
                        status = listing_response.status

                        logger.info(
                            msg=f"status={status}, url={listing_url}",
                            extra={
                                "elapsed_time": f"{elapsed_time:4f}",
                            }
                        )

                        file_path = f"./data/bizbuysell.csv"

                        # If the file does not exist, create it and write the headers
                        if not os.path.isfile(file_path):
                            with open(file_path, 'w', newline='') as f:
                                writer = csv.DictWriter(f, fieldnames=data.keys())
                                writer.writeheader()

                        # Append the data
                        with open(file_path, 'a', newline='') as f:
                            writer = csv.DictWriter(f, fieldnames=data.keys())
                            writer.writerow(data)

                    # After scraping, add the URL to the list of scraped URLs
                    with open('scraped_urls.txt', 'a') as f:
                        f.write(listing_url + '\n')  # Write listing_url, not u


async def dispatch(url, settings):
    await HTTPClientDownloader(url, settings)


async def job(start_urls, settings):
    tasks = []
    for url in start_urls:
        task = asyncio.create_task(dispatch(url, settings))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    print(f"total requests", len(results))

async def main():
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

    start_urls = [f"https://www.bizbuysell.com/businesses-for-sale/{i}" for i in range(1, 201)] # only 200 pages are displayed at a time
    tasks = []
    task = asyncio.create_task(job(start_urls, settings))  # create tasks for each job, not main
    tasks.append(task)

    await asyncio.gather(*tasks)  # Gather and run all tasks
    await asyncio.sleep(86400)  # Sleep for one day

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
    loop.close()