from snowflake.cortex import Complete,ExtractAnswer,Sentiment,Translate,EmbedText1024,EmbedText768,ClassifyText,complete
from snowflake.snowpark import Session



def summarize_article(user_text, session, query):
    PROMPT_TEMPLATE = f"""
    You are a financial expert and sentiment analysis assistant. Based on the following news article, identify and summarize the financial sentiment 
    and technical insights related to the company or stock mentioned. The article provided is about {query}. Focus on trends, market sentiment, potential 
    impact on stock performance, and other technical details that could guide a trader's decisions.Also Include the financial numbers in the article to get
    a better understanding of the company's financial health.

    Article:
    {user_text}

    Please present your findings in a structured format with bullet points. Include any market movements, key metrics,
    and other important technical insights that could affect trading strategies.
    """
    
    completion = Complete(
        model="mistral-large2",
        prompt=PROMPT_TEMPLATE,
        session=session,
        options={
            'temperature': 0.7,    # This controls the creativity of the responses
            # 'max_tokens': 100   # You can adjust this if necessary to fit your response size.
        }
    )
    return completion



def analyze_financial_sentiment(user_text, session):
    # The prompt should focus on analyzing the financial sentiment of the article
    PROMPT_TEMPLATE = f"""
    You are a financial sentiment analysis expert. Based on the given article :- {user_text}, identify the overall sentiment in terms of financial markets.
    Classify the sentiment as Positive, Negative, or Neutral.

    Financial sentiment analysis (with key reasons). Note:provide only 4 points:
    """

    sentiment_result = complete(
        model="mistral-large2",
        prompt=PROMPT_TEMPLATE,
        session=session,
        options={
            'temperature': 0.2,  # Controls the randomness of the sentiment analysis output
        }
    )
    return sentiment_result
