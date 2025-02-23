import pandas as pd
import requests
import streamlit as st
from src.logger import logging
from src.exception import CustomException
import time



# Setting up custom logger
# setup_logger()

class NewsExtractor:
    def __init__(self):
        try:
            self.news_api_key = st.secrets["newsapi"]["apikey"]
            self.diffbot_api_token = st.secrets["difbot"]["apikey"]
        except Exception as e :
            logging.error(f"Error initializing newsapi and diffbot secrets {CustomException(e)}")
            raise CustomException(e)

    def __fetch_news(self, query, from_date=None, to_date=None, language='en', sort_by='relevancy', max_retries=3): 
        """Fetches news articles using NewsAPI with proper logging and exception handling."""
        
        url = "https://newsapi.org/v2/everything"
        headers = {"Authorization": f"Bearer {self.news_api_key}"}
        params = {
            "q": f'"{query}"',
            "from": from_date,
            "to": to_date,
            "language": language,
            "sortBy": sort_by,
            "pageSize": 100
        }

        attempt = 0
        while attempt < max_retries:
            try:
                logging.info(f"Fetching news for query: {query}, Attempt: {attempt + 1}")
                response = requests.get(url, headers=headers, params=params, timeout=10)
                
                if response.status_code != 200:
                    logging.error(f"Error fetching news (Status Code: {response.status_code}) - {response.text}")
                    response.raise_for_status()

                data = response.json()
                articles = data.get("articles", [])
                logging.info(f"Successfully fetched {len(articles)} articles for query: {query}")
                return pd.DataFrame(articles)

            except requests.exceptions.RequestException as e:
                logging.error(f"Request failed: {e}", exc_info=True)
                attempt += 1
                time.sleep(2 ** attempt)  # Exponential backoff
            
            except Exception as e:
                logging.critical(f"Unexpected error: {e}", exc_info=True)
                break  # No retry for unexpected errors
        
            logging.error(f"Failed to fetch news after {max_retries} attempts for query: {query}")
            return pd.DataFrame()  # Return empty DataFrame in case of failure

    def __extract_news_content(self, url):
        api_url = 'https://api.diffbot.com/v3/article'
        params = {'token': self.diffbot_api_token, 'url': url}
        try:
            response = requests.get(api_url, params=params)
            if response.status_code == 200:
                data = response.json()
                article = data.get('objects', [{}])[0]
                if article:
                    return article.get('title', 'No title'), article.get('text', 'No content').strip()
                else:
                    return None, "No article content found."
            else:
                return None, f"Error: {response.status_code}"
        except Exception as e:
            return None, str(e)

    def process_news(self, query):
        """Processes fetched news articles by filtering, transforming, and extracting content."""
    
        try:
            logging.info(f"Starting news processing for query: {query}")
            
            # Fetch news
            newsdataframe = self.__fetch_news(query, language='en', sort_by='relevancy')
            
            if newsdataframe.empty:
                logging.warning(f"No news articles found for query: {query}")
                return newsdataframe

            # Drop unnecessary columns
            drop_cols = ['urlToImage', 'content']
            newsdataframe.drop(columns=[col for col in drop_cols if col in newsdataframe.columns], inplace=True, errors='ignore')
            
            # Convert date format
            newsdataframe['publishedAt'] = newsdataframe['publishedAt'].astype(str)
            newsdataframe['date'] = pd.to_datetime(newsdataframe['publishedAt'].str.split('T').str[0], errors='coerce')
            newsdataframe.drop(columns=['publishedAt'], inplace=True, errors='ignore')

            # Extract source names safely
            newsdataframe['source'] = newsdataframe['source'].apply(lambda x: x.get('name', 'Unknown') if isinstance(x, dict) else 'Unknown')

            # Remove duplicates
            newsdataframe.drop_duplicates(subset=['description'], inplace=True)
            
            # Get latest 15 articles
            df_temp = newsdataframe.sort_values(by='date', ascending=False).head(15)
            
            logging.info(f"Processing {len(df_temp)} articles for content extraction.")

            # Extract full content from URLs
            try:
                results = df_temp['url'].apply(lambda x: self.__extract_news_content(x) if isinstance(x, str) else "Error: Invalid URL")
                df_temp[['title', 'content']] = pd.DataFrame(results.tolist(), index=df_temp.index)
            except Exception as e:
                logging.error(f"Error extracting news content: {e}", exc_info=True)
                df_temp[['title', 'content']] = "Error", "Error"

            # Remove rate-limited articles
            df_temp = df_temp[df_temp['content'] != "Error: 429"].reset_index(drop=True)
            
            # Add unique ID column
            df_temp["id"] = df_temp.index
            df_temp = df_temp[[df_temp.columns[-1]] + list(df_temp.columns[:-1])]

            logging.info(f"Successfully processed {len(df_temp)} articles for query: {query}")
            return df_temp

        except Exception as e:
            logging.critical(f"Unexpected error while processing news for query {query}: {e}", exc_info=True)
            return pd.DataFrame()  # Return empty DataFrame on failure