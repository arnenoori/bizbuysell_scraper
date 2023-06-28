# BizBuySell Scraper

This script is used to scrape business listing data from the website BizBuySell. The data it scrapes includes the asking price, cash flow, gross revenue, and other relevant business details for each listing.

## Dependencies

The script requires Python 3.7 or later and the following Python libraries:

- `beautifulsoup4`
- `python-json-logger`
- `aiolimiter`
- `aiohttp`
- `aiofiles`
- `nest_asyncio` (only if running in a Jupyter notebook or IPython shell)

You can install these dependencies using pip:

```bash
pip install beautifulsoup4 python-json-logger aiolimiter aiohttp aiofiles nest_asyncio
```

## How it Works

The script operates in two stages:

1. It first navigates to a list of business listings on BizBuySell, and extracts the URLs for each individual business listing on that page.

2. It then visits each individual listing URL, and extracts the detailed business data from that page.

The script uses asyncio and aiohttp to perform these tasks asynchronously for efficiency, and uses aiolimiter to rate limit requests to BizBuySell to prevent overloading the server or getting banned. It also randomizes the delay between requests to further avoid detection.

The extracted data is saved to a CSV file in a directory called data. The CSV file includes the following columns: Title, Location, Asking Price, Cash Flow, Gross Revenue, EBITDA, Established, Inventory, FF&E, Business Description, URL, and other detailed info.

A log of all scraped URLs is kept in a file called scraped_urls.txt to prevent re-scraping the same pages in subsequent runs.

The script uses beautifulsoup4 to parse the HTML of the web pages, and python-json-logger to log events in a machine-readable JSON format.

The script is designed to run indefinitely and perform the scraping task every 24 hours (86400 seconds).

After each cycle of scraping, the script pauses for 24 hours before it begins the next cycle. This means that the script will continue to run, checking for new listings and adding them to the CSV every 24 hours.

As for how it checks for new listings, the script keeps track of which URLs have already been scraped in a text file called scraped_urls.txt. Before scraping a URL, it checks this file to see if the URL is already there. If it is, the script skips that URL and moves on to the next one. If it's not, the script proceeds to scrape the URL, add the data to the CSV, and add the URL to scraped_urls.txt. This way, the script avoids re-scraping the same pages and only adds new listings to the CSV.

Keep in mind that the script does not automatically remove listings from the CSV that are no longer on the website. It only adds new listings. If you need to keep track of which listings are currently active, you may need to modify the script or use a separate process to remove listings from the CSV that are no longer active on the website.

## Usage

To use the script, you need to specify the states you want to scrape in the states list in the main section of the script.

Then, simply run the script from the command line:
```bash
python bizbuysell_scraper.py
```