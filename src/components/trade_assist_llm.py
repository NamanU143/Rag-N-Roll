from snowflake.cortex import Complete,ExtractAnswer,Sentiment,Translate,EmbedText1024,EmbedText768,ClassifyText,complete
from snowflake.snowpark import Session



def summarize_article(user_text,session):
    PROMPT_TEMPLATE = f"""
    You are an expert trading assistant. Based on the following article, identify and summarize five key points
    that can assist a trader in making informed decisions.Focus on actionable insights, trends, and relevant data
    that could impact trading strategies.

    Article:
    {user_text}

    Please present your findings in a structured format with bullet points.
    """
    
    completion = Complete(
        model="mistral-large2",
        prompt=PROMPT_TEMPLATE,
        session=session,
        options={
            'temperature': 0.7,    #this parameter controls the randomness of the responses (higher values meaning more creative)
            # 'max_tokens': 100   # sets limit to the gererated responses    this cant be higher than the model maxtoken generation.     
        }
    )
    return completion


def analyze_financial_sentiment(user_text,session):
    # The prompt should focus on analyzing the financial sentiment of the article
    PROMPT_TEMPLATE = f"""
    You are a financial sentiment analysis expert. Based on the following article, identify the overall sentiment in terms of financial markets.
    Classify the sentiment as Positive, Negative, or Neutral.Dont explain anything .

    Article:
    {user_text}

    Financial sentiment analysis:
    """

    sentiment_result = Complete(
        model="mistral-large2",
        prompt=PROMPT_TEMPLATE,
        session=session,
        options={
            'temperature': 0.7,  # Controls the randomness of the sentiment analysis output
        }
    )
    return sentiment_result