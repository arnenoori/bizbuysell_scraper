# BizBuySell Scraper

This script is used to scrape business listing data from the website BizBuySell. The data it scrapes includes the asking price, cash flow, gross revenue, and other relevant business details for each listing.

## Dependencies

The script requires Python 3.7 or later and the following Python libraries:

- `beautifulsoup4`
- `python-json-logger`
- `aiolimiter`
- `aiohttp`
- `aiofiles`

You can install these dependencies using pip:

```bash
pip install beautifulsoup4 python-json-logger aiolimiter aiohttp aiofiles
```

## How it Works

The script operates in two stages:

1. It first navigates to a list of business listings for a specified state on BizBuySell, and extracts the URLs for each individual business listing on that page.

2. It then visits each individual listing URL, and extracts the detailed business data from that page.

The script uses asyncio and aiohttp to perform these tasks asynchronously for efficiency, and uses aiolimiter to rate limit requests to BizBuySell to prevent overloading the server or getting banned. It also randomizes the delay between requests to further avoid detection.

The extracted data is saved to a CSV file in a directory called data, with one file per state. A log of all scraped URLs is kept in a file called scraped_urls.txt to prevent re-scraping the same pages.

The script uses beautifulsoup4 to parse the HTML of the web pages, and python-json-logger to log events in a machine-readable JSON format.

## Usage

To use the script, you need to specify the states you want to scrape in the states list in the main section of the script.

Then, simply run the script from the command line:
```bash
python bizbuysell_scraper.py
```