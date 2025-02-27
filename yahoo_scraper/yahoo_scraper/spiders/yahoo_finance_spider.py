import scrapy
from scrapy.http import FormRequest
import datetime
from dateutil.parser import parse
import logging
import re
import json
import os

class YahooFinanceMessageBoardSpider(scrapy.Spider):
    name = 'yahoo_finance_messages'
    
    # Custom settings
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'DOWNLOAD_DELAY': 3,  # Increased delay
        'ROBOTSTXT_OBEY': True,
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_ENABLED': True,  # Enable cookies
        'DOWNLOAD_TIMEOUT': 30,
        'HTTPERROR_ALLOWED_CODES': [404],
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }
    }
    
    def __init__(self, *args, **kwargs):
        super(YahooFinanceMessageBoardSpider, self).__init__(*args, **kwargs)
        
        # Define the 10 stock symbols you want to scrape
        self.stock_symbols = [
            'AAPL',  # Apple
            'MSFT',  # Microsoft
            'GOOG',  # Google
            'AMZN',  # Amazon
            'FB',    # Facebook (now META, but was FB during your target period)
            'NFLX',  # Netflix
            'TSLA',  # Tesla
            'JPM',   # JPMorgan
            'WMT',   # Walmart
            'XOM',   # ExxonMobil
        ]
        
        # Define the date range
        self.start_date = datetime.datetime(2012, 7, 23)
        self.end_date = datetime.datetime(2013, 7, 19)
        
        # Create output directory if it doesn't exist
        if not os.path.exists('output'):
            os.makedirs('output')
    
    def start_requests(self):
        """Start the scraping process for each stock symbol."""
        for symbol in self.stock_symbols:
            # Just use the modern URL for now
            modern_url = f'https://finance.yahoo.com/quote/{symbol}/community?p={symbol}'
            
            yield scrapy.Request(
                url=modern_url,
                callback=self.parse_conversation_page,
                meta={'symbol': symbol},
                dont_filter=True,
                errback=self.handle_error
            )
    
    def handle_error(self, failure):
        self.logger.error(f"Request failed: {failure.value}")
        
    def parse_conversation_page(self, response):
        symbol = response.meta['symbol']
        self.logger.info(f"Processing conversations for {symbol}")
        
        # Create symbol directory
        symbol_dir = f'output/{symbol}'
        if not os.path.exists(symbol_dir):
            os.makedirs(symbol_dir)
            
        # Save raw HTML for debugging
        with open(f'{symbol_dir}/raw_page.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
            
        # Try to find the React app data
        scripts = response.xpath('//script/text()').getall()
        for script in scripts:
            if 'root.App.main' in script:
                try:
                    data_match = re.search(r'root\.App\.main\s*=\s*({.*});', script)
                    if data_match:
                        data = json.loads(data_match.group(1))
                        
                        # Save raw JSON data
                        with open(f'{symbol_dir}/raw_data.json', 'w', encoding='utf-8') as f:
                            json.dump(data, f, indent=2)
                            
                        # Try to extract messages
                        self.extract_messages_from_data(data, symbol)
                except Exception as e:
                    self.logger.error(f"Error processing script data for {symbol}: {str(e)}")
    
    def extract_messages_from_data(self, data, symbol):
        try:
            # Try different paths where message data might be stored
            stores = data.get('context', {}).get('dispatcher', {}).get('stores', {})
            conversation_store = stores.get('ConversationStore', {})
            
            if conversation_store:
                messages_data = conversation_store.get('messages', [])
                symbol_dir = f'output/{symbol}'
                
                for msg in messages_data:
                    try:
                        created_at = parse(msg.get('created_at', ''))
                        if self.start_date <= created_at <= self.end_date:
                            message = {
                                'symbol': symbol,
                                'message_id': msg.get('messageId'),
                                'user': msg.get('author', {}).get('username'),
                                'content': msg.get('content'),
                                'created_at': msg.get('created_at'),
                                'likes': msg.get('likes_count', 0),
                                'replies': msg.get('replies_count', 0)
                            }
                            
                            # Save message
                            filename = f"{symbol_dir}/message_{message['message_id']}.json"
                            with open(filename, 'w', encoding='utf-8') as f:
                                json.dump(message, f, indent=2)
                                
                            self.logger.info(f"Saved message {message['message_id']} for {symbol}")
                    except Exception as e:
                        self.logger.error(f"Error processing message: {str(e)}")
                        
        except Exception as e:
            self.logger.error(f"Error extracting messages for {symbol}: {str(e)}")

    def parse_legacy_board(self, response):
        """Parse the legacy message board page."""
        symbol = response.meta['symbol']
        self.logger.info(f"Processing legacy board for {symbol}")
        
        # Create symbol directory
        symbol_dir = f'output/{symbol}'
        if not os.path.exists(symbol_dir):
            os.makedirs(symbol_dir)
            
        # Save raw HTML for debugging
        with open(f'{symbol_dir}/legacy_raw_page.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        # Extract messages from the legacy page structure
        messages = response.css('table.msglist tr')
        
        for message in messages:
            # Skip header rows
            if message.css('th'):
                continue
                
            # Extract message data
            subject = message.css('td.subject a::text').get()
            if not subject:
                continue
                
            message_id = message.css('td.subject a::attr(href)').re_first(r'mid=(\d+)')
            user = message.css('td.author a::text').get() or 'Anonymous'
            date_str = message.css('td.date::text').get()
            
            if date_str:
                try:
                    date = parse(date_str)
                    # Check if within date range
                    if self.start_date <= date <= self.end_date:
                        message_data = {
                            'symbol': symbol,
                            'message_id': message_id,
                            'user': user,
                            'title': subject,
                            'created_at': date.isoformat()
                        }
                        
                        # Save message
                        filename = f"{symbol_dir}/legacy_message_{message_id}.json"
                        with open(filename, 'w', encoding='utf-8') as f:
                            json.dump(message_data, f, indent=2)
                            
                        self.logger.info(f"Saved legacy message {message_id} for {symbol}")
                        
                except Exception as e:
                    self.logger.error(f"Error processing legacy message: {str(e)}")
        
        # Check for next page
        next_page = response.css('a:contains("Next")::attr(href)').get()
        if next_page:
            yield response.follow(
                next_page,
                callback=self.parse_legacy_board,
                meta={'symbol': symbol}
            )


# Alternative approach using historical structure (if modern API doesn't work)
class YahooFinanceHistoricalMessageBoardSpider(scrapy.Spider):
    name = 'yahoo_finance_historical_messages'
    
    # Custom settings
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'DOWNLOAD_DELAY': 2,
        'ROBOTSTXT_OBEY': True,
        'CONCURRENT_REQUESTS': 1,
    }
    
    def __init__(self, *args, **kwargs):
        super(YahooFinanceHistoricalMessageBoardSpider, self).__init__(*args, **kwargs)
        
        # Define the 10 stock symbols you want to scrape
        self.stock_symbols = [
            'AAPL',  # Apple
            'MSFT',  # Microsoft
            'GOOG',  # Google
            'AMZN',  # Amazon
            'FB',    # Facebook (now META, but was FB during your target period)
            'NFLX',  # Netflix
            'TSLA',  # Tesla
            'JPM',   # JPMorgan
            'WMT',   # Walmart
            'XOM',   # ExxonMobil
        ]
        
        # Define the date range
        self.start_date = datetime.datetime(2012, 7, 23)
        self.end_date = datetime.datetime(2013, 7, 19)
        
        # Create output directory if it doesn't exist
        if not os.path.exists('output'):
            os.makedirs('output')
    
    def start_requests(self):
        """Start the scraping process for each stock symbol."""
        for symbol in self.stock_symbols:
            # This is the legacy URL structure for Yahoo Finance message boards
            # More likely to work for historical data from 2012-2013
            url = f'https://messages.finance.yahoo.com/mb/{symbol}'
            
            yield scrapy.Request(
                url=url, 
                callback=self.parse_legacy_board,
                meta={'symbol': symbol}
            )
    
    def parse_legacy_board(self, response):
        """Parse the legacy message board page."""
        symbol = response.meta['symbol']
        self.logger.info(f"Processing legacy message board for {symbol}")
        
        # Extract messages from the legacy page structure
        messages = response.css('table.msglist tr')
        
        for message in messages:
            # Skip header rows
            if message.css('th'):
                continue
                
            # Extract message data
            subject = message.css('td.subject a::text').get()
            if not subject:
                continue
                
            message_id = message.css('td.subject a::attr(href)').re_first(r'mid=(\d+)')
            user = message.css('td.author a::text').get() or 'Anonymous'
            date_str = message.css('td.date::text').get()
            
            if date_str:
                try:
                    date = parse(date_str)
                    # Check if within date range
                    if self.start_date <= date <= self.end_date:
                        # Get message details
                        message_url = f'https://messages.finance.yahoo.com/mb/{symbol}/message/{message_id}'
                        
                        yield scrapy.Request(
                            url=message_url,
                            callback=self.parse_legacy_message,
                            meta={
                                'symbol': symbol,
                                'message_id': message_id,
                                'subject': subject,
                                'user': user,
                                'date': date.isoformat()
                            }
                        )
                except Exception as e:
                    self.logger.error(f"Error parsing date {date_str}: {str(e)}")
        
        # Check for next page
        next_page = response.css('a:contains("Next")::attr(href)').get()
        if next_page:
            yield response.follow(
                next_page,
                callback=self.parse_legacy_board,
                meta={'symbol': symbol}
            )
    
    def parse_legacy_message(self, response):
        """Parse individual message details."""
        symbol = response.meta['symbol']
        message_id = response.meta['message_id']
        subject = response.meta['subject']
        user = response.meta['user']
        date = response.meta['date']
        
        # Extract message content
        content = ' '.join(response.css('div#message div.msgbody::text').getall())
        content = content.strip()
        
        # Save message data
        message_data = {
            'symbol': symbol,
            'message_id': message_id,
            'user': user,
            'title': subject,
            'content': content,
            'created_at': date
        }
        
        # Create symbol directory if it doesn't exist
        symbol_dir = f'output/{symbol}'
        if not os.path.exists(symbol_dir):
            os.makedirs(symbol_dir)
        
        # Save message to a JSON file
        filename = f"{symbol_dir}/message_{message_id}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(message_data, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"Saved message {message_id} for {symbol}")
        
        # Check for replies
        replies = response.css('ul.msglist li')
        for reply in replies:
            reply_user = reply.css('div.byuser span.username::text').get() or 'Anonymous'
            reply_date_str = reply.css('div.byuser::text').re_first(r'\((.*?)\)')
            reply_content = ' '.join(reply.css('div.msgbody::text').getall())
            reply_content = reply_content.strip()
            
            if reply_date_str:
                try:
                    reply_date = parse(reply_date_str)
                    # Check if within date range
                    if self.start_date <= reply_date <= self.end_date:
                        # Generate a unique ID for the reply
                        reply_id = f"{message_id}_{replies.index(reply)}"
                        
                        # Save reply data
                        reply_data = {
                            'symbol': symbol,
                            'message_id': reply_id,
                            'parent_id': message_id,
                            'user': reply_user,
                            'content': reply_content,
                            'created_at': reply_date.isoformat()
                        }
                        
                        # Save reply to a JSON file
                        filename = f"{symbol_dir}/reply_{reply_id}.json"
                        with open(filename, 'w', encoding='utf-8') as f:
                            json.dump(reply_data, f, ensure_ascii=False, indent=2)
                        
                        self.logger.info(f"Saved reply {reply_id} for message {message_id}")
                except Exception as e:
                    self.logger.error(f"Error parsing reply date {reply_date_str}: {str(e)}")