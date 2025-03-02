import scrapy
import datetime
import csv
import os
import json
import logging
import argparse
import time
from urllib.parse import urlencode


class YahooFinanceHistoricalDataSpider(scrapy.Spider):
    name = 'yahoo_finance_historical_data'


    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'DOWNLOAD_DELAY': 3,
        'ROBOTSTXT_OBEY': False,
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_ENABLED': True,
        'DOWNLOAD_TIMEOUT': 60,
    }

    def __init__(self, symbols=None, start_date=None, end_date=None, *args, **kwargs):
        super(YahooFinanceHistoricalDataSpider, self).__init__(*args, **kwargs)

        # Define the stock symbols you want to scrape (default or from user input)
        if symbols:
            self.stock_symbols = symbols.split(',')
        else:
            self.stock_symbols = [
                'AAPL',  # Apple
                'MSFT',  # Microsoft
                'GOOG',  # Google
                'AMZN',  # Amazon
                'META',  # Facebook (formerly FB)
                'NFLX',  # Netflix
                'TSLA',  # Tesla
                'JPM',  # JPMorgan
                'WMT',  # Walmart
                'XOM',  # ExxonMobil
            ]

        try:
            if start_date:
                self.start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
            else:
                # Default to one year ago
                self.start_date = datetime.datetime.now() - datetime.timedelta(days=365)

            if end_date:
                self.end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
            else:
                # Default to today
                self.end_date = datetime.datetime.now()

        except ValueError as e:
            self.logger.error(f"Date format error: {e}. Use YYYY-MM-DD format.")
            self.start_date = datetime.datetime.now() - datetime.timedelta(days=365)
            self.end_date = datetime.datetime.now()


        self.start_timestamp = int(self.start_date.timestamp())
        self.end_timestamp = int(self.end_date.timestamp())

        self.logger.info(f"Date range: {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}")
        self.logger.info(f"Symbols to scrape: {', '.join(self.stock_symbols)}")


        if not os.path.exists('output'):
            os.makedirs('output')

    def start_requests(self):
        """Start the scraping process for each stock symbol, using direct CSV download."""
        for symbol in self.stock_symbols:
            params = {
                'period1': self.start_timestamp,
                'period2': self.end_timestamp,
                'interval': '1d',
                'events': 'history',
                'includeAdjustedClose': 'true',
                'download': 'true'  # This is important for CSV download
            }

            url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?{urlencode(params)}"

            self.logger.info(f"Fetching historical data for {symbol} from {url}")

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/csv,application/csv,text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
                'Referer': f'https://finance.yahoo.com/quote/{symbol}/history',
                'Origin': 'https://finance.yahoo.com',
            }

            yield scrapy.Request(
                url=url,
                callback=self.parse_csv_download,
                meta={'symbol': symbol},
                headers=headers,
                errback=self.handle_error
            )

    def parse_csv_download(self, response):
        """Parse direct CSV download from Yahoo Finance."""
        symbol = response.meta['symbol']
        self.logger.info(f"Processing CSV download for {symbol}")

        try:

            content = response.body.decode('utf-8')

            if not content.startswith('Date,Open,High') and 'error' in content.lower():
                self.logger.error(f"Error in response for {symbol}: {content}")
                return

            output_file = f'output/{symbol}_historical_data.csv'
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                f.write(content)

            self.logger.info(f"Successfully saved CSV data for {symbol} to {output_file}")

        except Exception as e:
            self.logger.error(f"Error saving CSV data for {symbol}: {str(e)}")
            # Save raw response for debugging
            raw_dir = 'output/raw'
            if not os.path.exists(raw_dir):
                os.makedirs(raw_dir)

            with open(f'{raw_dir}/{symbol}_raw_csv_response.txt', 'wb') as f:
                f.write(response.body)

    def handle_error(self, failure):
        request = failure.request
        symbol = request.meta.get('symbol', 'unknown')
        self.logger.error(f"Request failed for {symbol}: {failure.value}")


        self.logger.info(f"Trying alternative method for {symbol}")
        time.sleep(10)

        # Try the v8 API endpoint as fallback
        params = {
            'period1': self.start_timestamp,
            'period2': self.end_timestamp,
            'interval': '1d',
            'events': 'history',
            'includeAdjustedClose': 'true'
        }
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?{urlencode(params)}"

        yield scrapy.Request(
            url=url,
            callback=self.parse_historical_data,
            meta={'symbol': symbol},
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Referer': f'https://finance.yahoo.com/quote/{symbol}/history',
            },
            dont_filter=True
        )

    def parse_historical_data(self, response):
        """Parse the JSON response containing historical data as fallback."""
        symbol = response.meta['symbol']
        self.logger.info(f"Processing historical data for {symbol}")

        try:

            data = json.loads(response.text)


            result = data.get('chart', {}).get('result', [])
            if not result or len(result) == 0:
                self.logger.error(f"No data found for {symbol}")
                return

            result = result[0]


            timestamps = result.get('timestamp', [])
            quote = result.get('indicators', {}).get('quote', [{}])[0]
            adjclose = result.get('indicators', {}).get('adjclose', [{}])[0]

            opens = quote.get('open', [])
            highs = quote.get('high', [])
            lows = quote.get('low', [])
            closes = quote.get('close', [])
            volumes = quote.get('volume', [])
            adj_closes = adjclose.get('adjclose', []) if adjclose else []

            # Prepare CSV file
            output_file = f'output/{symbol}_historical_data.csv'
            with open(output_file, 'w', newline='') as csvfile:
                if adj_closes:
                    fieldnames = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
                else:
                    fieldnames = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']

                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                # Write data rows
                for i in range(len(timestamps)):
                    if i < len(opens) and opens[i] is not None:
                        date = datetime.datetime.fromtimestamp(timestamps[i]).strftime('%Y-%m-%d')

                        row = {
                            'Date': date,
                            'Open': opens[i],
                            'High': highs[i],
                            'Low': lows[i],
                            'Close': closes[i],
                            'Volume': volumes[i]
                        }

                        if adj_closes and i < len(adj_closes):
                            row['Adj Close'] = adj_closes[i]

                        writer.writerow(row)

            self.logger.info(f"Successfully saved historical data for {symbol} to {output_file}")

        except Exception as e:
            self.logger.error(f"Error processing historical data for {symbol}: {str(e)}")
            raw_dir = 'output/raw'
            if not os.path.exists(raw_dir):
                os.makedirs(raw_dir)

            with open(f'{raw_dir}/{symbol}_raw_json_response.txt', 'w', encoding='utf-8') as f:
                f.write(response.text)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape historical stock data from Yahoo Finance')
    parser.add_argument('--symbols', type=str, help='Comma-separated list of stock symbols (e.g. AAPL,MSFT,GOOG)')
    parser.add_argument('--start', type=str, help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end', type=str, help='End date in YYYY-MM-DD format')

    args = parser.parse_args()

    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings

    process = CrawlerProcess(get_project_settings())
    process.crawl(YahooFinanceHistoricalDataSpider,
                  symbols=args.symbols,
                  start_date=args.start,
                  end_date=args.end)
    process.start()