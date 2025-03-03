from snowflake.snowpark import Session
from src.logger import logging
from src.configuration.snowflake import SnowflakeConnector

conn = SnowflakeConnector()
session = conn.get_session()

def summarize_article(article_text, focus_topic, user_query):
    prompt = f"""
        You are a financial expert. Summarize the following article focusing on {focus_topic}. Extract key financial and trading insights.

        **Article:**
        {article_text}

        **Summary:**
        - **Stock Performance & Market Reaction:** (Price movements, investor sentiment, and market impact.)
        - **Key Financial Metrics:** (Earnings, revenue, debt, P/E ratio, etc.)
        - **Industry Trends & Macro Factors:** (Regulatory impact, sector trends.)
        - **Company-Specific News:** (Mergers, leadership changes, product launches.)
        - **Sentiment Analysis:** (Classify sentiment as Bullish, Bearish, or Neutral.)
        """

    sql_query = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT('role', 'user', 'content', {prompt})
        ),
        OBJECT_CONSTRUCT(
            'temperature', 0.7,
            'max_tokens', 300,
            'stream',true
        )
    )
    """
    
    result = session.sql(sql_query).collect()
    return result[0][0] if result else "No response"


def analyze_financial_sentiment(user_text, session, user_query):
    PROMPT_TEMPLATE = f"""
    You are an expert financial sentiment analysis AI. Your task is to analyze the given financial news article and classify its sentiment accurately.

    **Article for Analysis:**
    "{user_text}"

    **Instructions:**
    - Identify the overall sentiment of the article based on its financial tone.
    - Display only the sentiment results as specified below.
    
    **Response Format (Strictly follow this format):**
    News Article Sentiment: [Positive / Slightly Positive / Neutral / Slightly Negative / Negative]\n
    Sentiment Direction: [Positive to Neutral / Neutral to Positive / Neutral to Negative / Negative to Neutral / Positive to Negative / Negative to Positive/Neutral]\n
    Reasoning : (Give reason for the specific sentiment and sentiment direction in less than 4 points)
    **Note:** Do not include any explanations or additional details. Only return the sentiment classifications and Reasoning.
    """

    sql_query = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT('role', 'user', 'content', {PROMPT_TEMPLATE})
        ),
        OBJECT_CONSTRUCT(
            'temperature', 0.7,
            'max_tokens', 300
        )
    )
    """
    
    result = session.sql(sql_query).collect()
    return result[0][0] if result else "No response"



# predict_trend-Predict short-term & long-term stock impact from news
# assess_risk-Measure risk level and recommend Buy/Hold/Sell
# competitive_analysis-Compare news sentiment between two stocks
# regulatory_impact-Identify if an article suggests regulatory risks
# extract_event_timeline-Extract financial events with dates from news





# def streamed_output(user_text, session, query):
#     # The prompt that instructs the model to generate structured financial insights
#     PROMPT_TEMPLATE = f"""
#     You are a financial expert and sentiment analysis assistant. Based on the following news article, identify and summarize the financial sentiment 
#     and technical insights related to the company or stock mentioned. The article provided is about {query}. Focus on trends, market sentiment, potential 
#     impact on stock performance, and other technical details that could guide a trader's decisions. Also, include the financial numbers in the article to get
#     a better understanding of the company's financial health.

#     Article:
#     {user_text}

#     Please present your findings in a structured format with bullet points. Include any market movements, key metrics,
#     and other important technical insights that could affect trading strategies.
#     """
    
#     # Initialize a Streamlit placeholder to display the output incrementally
#     output_placeholder = st.empty()  # Placeholder to update content in real-time

#     # Streaming the response using Snowflake Cortex API (assuming it is properly set up)
#     stream = Complete(
#         model="mistral-large2",
#         prompt=PROMPT_TEMPLATE,
#         session=session,
#         options={
#             'temperature': 0.7,
#             'stream': True  # Enables streaming of the output in real-time
#         }
#     )

#     # Collect and display the streamed output
#     accumulated_output = ""
#     for update in stream:
#         print(update)
#         accumulated_output += update  # Append the streamed update to the accumulated output
#         output_placeholder.text(accumulated_output)  # Update the Streamlit placeholder in real-time
    
#     # Final clean-up and return the result
#     result = accumulated_output.strip().replace("\n", " ").replace("\r", "")
#     # print(result)
#     return result