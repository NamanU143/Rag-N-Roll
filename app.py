import streamlit as st
import pandas as pd
from src.configuration.snowflake import SnowflakeConnector
from src.components.news_extraction import NewsExtractor
from components.trade_assist_llm_sql import summarize_article, analyze_financial_sentiment
from components.cortex_response_extractor import FieldExtractor
from src.components.insert_data_into_db import SnowflakeDataInserter
from src.components.database_manager import SnowflakeDatabaseManager
from components.format_df import SnowflakeDataTypeMapper
from src.components.create_cortex_search_service import CortexSearchServiceManager
from snowflake.core import Root
from src.logger import setup_logger
import logging

# Setting up custom logger
setup_logger()

# Initialize Snowflake session
logging.info("<< Initializing Snowflake Session")
snowflake_connector = SnowflakeConnector()
session = snowflake_connector.get_session()

# Ensure database and schema context is set
DB_NAME = "MAIN_RAG_DB"
SCHEMA_NAME = "MAIN_RAG_SCHEMA"
session.sql(f"USE DATABASE {DB_NAME}").collect()
session.sql(f"USE SCHEMA {SCHEMA_NAME}").collect()
root = Root(session)
logging.info("Snowflake Session Established >> ")

# Set Streamlit page configuration
st.set_page_config(page_title="Trading Assistant", layout="wide")

# Title and description
st.title("Trading Assistant - Stock Insights")
st.markdown("This app helps traders by providing insights based on stock-related news articles.")

# Define the news query input
stock_query = st.text_input("Enter Stock Symbol or Company (e.g., Tesla)", "Tesla")

# API Keys for News Extraction
NEWS_API_KEY = "5eb7fffe17364f9bbce3e17dbeddd867"
DIFFBOT_API_TOKEN = "3130e6ff0e12e6877f3b7739d440d539"

# Create a button to fetch stock news
if st.button("Fetch Stock News"):
    try:
        # Fetching stock news using NewsExtractor
        news_extractor = NewsExtractor(NEWS_API_KEY, DIFFBOT_API_TOKEN)
        stock_news_df = news_extractor.process_news(stock_query)

        # Preprocessing step
        if not stock_news_df.empty:
            stock_news_df.drop(columns=["date"], inplace=True)  # Remove unnecessary columns
            stock_news_df.columns = stock_news_df.columns.str.upper()

            st.write("Fetched Stock News:")
            st.dataframe(stock_news_df)

            # Creating an instance of SnowflakeDatabaseManager
            db_manager = SnowflakeDatabaseManager(session)
            db_manager.create_database(DB_NAME)
            db_manager.create_schema(DB_NAME, SCHEMA_NAME)

            # Create table
            table_name = "STOCK_NEWS"
            db_manager.create_table(DB_NAME, SCHEMA_NAME, stock_news_df)

            # Write data to Snowflake
            session.write_pandas(stock_news_df, table_name, auto_create_table=False)

            # Summarization and sentiment analysis
            context = " ".join(stock_news_df["CONTENT"].tolist())
            sentiment_result = analyze_financial_sentiment(context, session=session)
            summary_result = summarize_article(context, session=session)

            # Display results
            st.subheader("Key Points for Traders")
            st.write(summary_result)
            
            st.subheader("Financial Sentiment Analysis")
            st.write(f"Sentiment: {sentiment_result}")
        else:
            st.warning("No news found for the given query.")

    except Exception as e:
        st.error(f"Error fetching stock news: {e}")

# Option to store results into Snowflake
if st.button("Save Results to Snowflake"):
    try:
        # Prepare data for insertion
        data_to_insert = {
            "Stock Symbol": stock_query,
            "Article Summary": summary_result.result["choices"][0]["text"],
            "Sentiment": sentiment_result.result["choices"][0]["text"],
        }
        results_df = pd.DataFrame([data_to_insert])

        # Insert data into Snowflake
        snowflake_inserter = SnowflakeDataInserter(session)
        snowflake_inserter.insert_df(results_df, "stock_insights_table")

        st.success("Results successfully inserted into Snowflake.")
    except Exception as e:
        st.error(f"Error inserting results into Snowflake: {e}")

# Close the Snowflake session when the app is stopped
if st.button("Close Session"):
    snowflake_connector.close_session()
    st.success("Snowflake session closed.")
