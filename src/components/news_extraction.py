import pandas as pd
import requests

class NewsExtractor:
    def __init__(self, news_api_key, diffbot_api_token):
        self.news_api_key = news_api_key
        self.diffbot_api_token = diffbot_api_token

    def fetch_news(self, query, from_date=None, to_date=None, language='en', sort_by='relevancy'):
        url = "https://newsapi.org/v2/everything"
        headers = {"Authorization": f"Bearer {self.news_api_key}"}
        params = {
        "q": f'"{query}"',  # Using exact match to get relevant results
        "from": from_date,
        "to": to_date,
        "language": language,
        "sortBy": sort_by,
        "pageSize": 100  # Max results per request
        }
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            raise Exception(f"Error fetching news: {response.status_code}, {response.json()}")
        data = response.json()
        articles = data.get("articles", [])
        filtered_articles = [
            article for article in articles
            if query.lower() in article.get("title", "").lower() or
                query.lower() in article.get("description", "").lower()
        ]
        return pd.DataFrame(articles)

    def extract_news_content(self, url):
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
        newsdataframe = self.fetch_news(query, language='en', sort_by='relevancy')
        newsdataframe.drop(columns=['urlToImage', 'content'], inplace=True)
        newsdataframe['publishedAt'] = newsdataframe['publishedAt'].astype(str)
        newsdataframe['date'] = pd.to_datetime(newsdataframe['publishedAt'].str.split('T').str[0])
        newsdataframe.drop(columns=['publishedAt'], inplace=True)
        newsdataframe['source'] = newsdataframe['source'].apply(lambda x: x['name'])
        newsdataframe.drop_duplicates(subset=['description'], inplace=True)
        df_temp = newsdataframe.sort_values(by='date', ascending=False).head(15)
        results = df_temp['url'].apply(lambda x: self.extract_news_content(x))
        df_temp[['title', 'content']] = pd.DataFrame(results.tolist(), index=df_temp.index)
        df_temp = df_temp[df_temp['content'] != "Error: 429"].reset_index(drop=True)
        df_temp["id"] = df_temp.index
        df_temp = df_temp[[df_temp.columns[-1]] + list(df_temp.columns[:-1])]
        return df_temp

# if __name__ == "__main__":
#     API_KEY = "5eb7fffe17364f9bbce3e17dbeddd867"
#     DIFFBOT_API_TOKEN = "3130e6ff0e12e6877f3b7739d440d539"
#     query = "Tesla"
#     extractor = NewsExtractor(API_KEY, DIFFBOT_API_TOKEN)
#     df_temp = extractor.process_news(query)
#     print(df_temp)
