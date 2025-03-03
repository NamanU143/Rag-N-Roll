import streamlit as st
from snowflake.snowpark import Session
from snowflake.cortex import Complete,complete
from src.configuration.snowflake import SnowflakeConnector

def get_snowflake_session():
    connector = SnowflakeConnector()
    return connector.get_session()

def summarize(model="mistral-7b", query=None,article_text:str=None,focus_topic:str=None):
    session = get_snowflake_session()
    
    # article_text = f"""
    # Tesla Inc. (TSLA) saw a 7% surge in its stock price after reporting better-than-expected Q4 earnings on Wednesday. The company posted a net income of $3.9 billion, exceeding Wall Street estimates. Revenue rose 15% year-over-year to $25.2 billion, driven by record vehicle deliveries and strong demand for its Model Y and Model 3.
    # CEO Elon Musk stated that Tesla is on track to produce 2 million vehicles in 2025, with increased production capacity in Gigafactories across Texas and Berlin. However, the company warned of macroeconomic challenges, including rising interest rates affecting vehicle affordability.
    # Despite concerns, analysts remain optimistic, with Morgan Stanley upgrading Tesla's stock outlook from Neutral to Overweight, citing strong demand for EVs and Tesla's advancements in AI-driven self-driving technology.
    # """

    # focus_topic = f"Tesla's Stock Surges After Strong Q4 Earnings Report"

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

    return complete(model, prompt=prompt, session=session, stream=True)








#######################################################################################################################################
#######################################################################################################################################

# from langchain_community.chat_models import ChatSnowflakeCortex
# from langchain_core.messages import HumanMessage, SystemMessage
# from src.configuration.snowflake import SnowflakeConnector


# conn = SnowflakeConnector()
# session = conn.get_session()

# chat = ChatSnowflakeCortex(
#     # Change the default cortex model and function
#     model="mistral-large",
#     cortex_function="complete",

#     # Change the default generation parameters
#     temperature=0,
#     max_tokens=10,
#     top_p=0.95,

#     session=session,
#     database="MAIN_RAG_DB",
#     schema="MAIN_RAG_SCHEMA"
# )


# messages = [
#     SystemMessage(content="You are a friendly assistant."),
#     HumanMessage(content="What are large language models?"),
# ]

# # Invoke the stream method and print each chunk as it arrives
# print("Stream Method Response:")
# for chunk in chat._stream(messages):
#     print(chunk.message.content)

