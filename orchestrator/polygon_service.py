"""Service for interacting with Polygon API."""
from typing import List
import logging
import os
from polygon import RESTClient

logger = logging.getLogger(__name__)


class PolygonService:
    """Handles interactions with Polygon API."""
    
    def __init__(self):
        self.client = None
        
    def initialize(self):
        """Initialize Polygon client with API key."""
        api_key = os.getenv("POLYGON_API_KEY")
        if not api_key:
            logger.error("POLYGON_API_KEY environment variable not set")
            raise ValueError("POLYGON_API_KEY is required")
        
        self.client = RESTClient(api_key=api_key)
        logger.info("Polygon client initialized")
    
    def fetch_stock_list(self) -> List[str]:
        """Fetch list of all US stocks from Polygon."""
        if not self.client:
            raise ValueError("Polygon client not initialized")
        
        try:
            logger.info("Fetching stock list from Polygon...")
            
            tickers = []
            next_url = None
            page_count = 0
            
            while True:
                if next_url:
                    response = self.client.list_tickers(next_url=next_url)
                else:
                    response = self.client.list_tickers(
                        market='stocks',
                        active=True,
                        limit=1000
                    )

                for ticker in response:
                    if hasattr(ticker, 'locale') and ticker.locale == 'us':
                        symbol = ticker.ticker

                        if (symbol and
                            not symbol.startswith('$') and
                            not symbol.startswith('^')):
                            tickers.append(symbol)
                
                page_count += 1
                logger.info(f"Page {page_count}: Fetched {len(tickers)} tickers so far...")

                if hasattr(response, 'next_url') and response.next_url:
                    next_url = response.next_url
                else:
                    break
            
            logger.info(f"Fetched {len(tickers)} US stocks from Polygon across {page_count} pages")
            return tickers
            
        except Exception as e:
            logger.error(f"Error fetching stocks from Polygon: {str(e)}")
            raise
