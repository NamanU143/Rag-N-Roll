import logging
import requests
import pandas as pd
from src.exception import CustomException

# API details
url = "https://api.exa.ai/search"

class StockNewsFetcher:
    """
    Class for fetching stock-related news from an external API.
    """

    def __init__(self,query):
        try:
            self.query = query
        except Exception as e:
            raise CustomException("Invalid query",e)

    def fetch_news_exa(self):
        """
        Fetch stock news from the API.

        Returns:
            pd.DataFrame: DataFrame containing the stock news data.
        """
        try:
            logging.info("Making API request to fetch stock news.")
            response = requests.request("POST", url, json=self.payload, headers=self.headers)

            # Check for a successful response
            if response.status_code == 200:
                data = response.json()  # Parse the JSON response
                
                # Check the structure of the response
                if 'results' in data:  # Assuming 'results' contains the relevant data
                    df = pd.DataFrame(data['results'])
                    logging.info(f"Fetched {len(df)} rows of stock news.")
                else:
                    df = pd.DataFrame(data)  # Handle if the response is already a flat list or dict
                    logging.warning("No 'results' found in the response. Returning raw data.")
                
                return df
            else:
                error_msg = f"Error: {response.status_code}, {response.text}"
                logging.error(error_msg)
                raise CustomException(error_msg)
        except Exception as e:
            logging.error(f"Error fetching stock news: {e}")
            raise CustomException(f"Error fetching stock news: {str(e)}")
        
    # def fetch_news_tavily(self):




