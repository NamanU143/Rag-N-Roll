import streamlit as st
import pandas as pd
from src.configuration.snowflake import SnowflakeConnector
from src.components.news_articles_extraction import StockNewsFetcher
from src.components.llm_response_extractor import FieldExtractor
from src.components.insert_data_into_db import SnowflakeDataInserter
from src.components.database_manager import SnowflakeDatabaseManager
from src.components.format_df import SnowflakeDataTypeMapper
from src.components.create_cortex_search_service import CortexSearchServiceManager
from snowflake.core import Root
from src.components.llm_response_extractor import FieldExtractor
from src.components.trade_assist_llm import summarize_article
from src.components.trade_assist_llm import analyze_financial_sentiment
from src.logger import setup_logger
import logging


# Setting up custom logger
setup_logger()

# Initialize Snowflake session
logging.info("<< Initializing Snowflake Session")
snowflake_connector = SnowflakeConnector()
session = snowflake_connector.get_session()
root = Root(session)
logging.info("Snowflake Session Established >> ")


# Set Streamlit page configuration
st.set_page_config(page_title="Trading Assistant", layout="wide")

# Title and description
st.title("Trading Assistant - Stock Insights")
st.markdown("This app helps traders by providing insights based on stock-related news articles.")

# Define the news query input
stock_query = st.text_input("Enter Stock Symbol or Company (e.g., Tesla)", "Tesla")

# Create a button to fetch stock news
if st.button("Fetch Stock News"):
    try:

        # -----------------------------------------------------------

        # Fetching stock news
        news_fetcher = StockNewsFetcher(stock_query)
        stock_news_df = news_fetcher.fetch_news_exa()
        print(stock_news_df)

        # -----------------------------------------------------------

        # Preprocessing step
        stock_news_df.drop(columns=["favicon","image","id"],inplace=True)
        stock_news_df["id"] = stock_news_df.index # adding index for unique identifier preprocessing step
        stock_news_df.insert(0,"id",stock_news_df.pop("id"))
        stock_news_df.columns = stock_news_df.columns.str.upper() # preprocessing step
        print(stock_news_df.columns)
        print(stock_news_df)

        if not stock_news_df.empty:
            # -----------------------------------------------------------

            st.write("Fetched Stock News:")
            st.dataframe(stock_news_df)

            # -----------------------------------------------------------
            # Creating an instance of SnowflakeDatabaseManager
            db_manager = SnowflakeDatabaseManager(session)

            # Step 1: Create database
            db_name = "MAIN_RAG_DB"
            db_manager.create_database(db_name)

            # -----------------------------------------------------------
            # Step 2: Create schema
            schema_name = "MAIN_RAG_SCHEMA"
            db_manager.create_schema(db_name, schema_name)


            # -----------------------------------------------------------
            
            # Step 3: Create table

            # Mapping dataframe columns to snowflake data types

            # Initialize the class
            data_type_mapper = SnowflakeDataTypeMapper()

            # Map the DataFrame columns to Snowflake-compatible data types
            columns = data_type_mapper.get_column_data_types(stock_news_df)

            # Print or use the resulting column data types
            print(columns)

            table_name = "MY_NEW_TABLE"
            db_manager.create_table(db_name, schema_name, table_name, columns)

            # -----------------------------------------------------------
            # Step 4: Insert data into snowflake table

            # Write the DataFrame to a Snowflake table
            table_name = "MY_NEW_TABLE"  # Specify your table name here

            # Use write_pandas to insert the DataFrame into the specified tableA
            snowpark_df = session.write_pandas(stock_news_df, table_name, auto_create_table=False)

            # -----------------------------------------------------------


            service_manager = CortexSearchServiceManager(session)

            # Define parameters for creating the Cortex Search service
            service_name = "news_search"
            table_name = "MY_NEW_TABLE"
            text_column = "TEXT"
            attributes = ["AUTHOR", "ID", "PUBLISHEDDATE", "SUMMARY", "TITLE", "URL"]
            warehouse = "COMPUTE_WH"
            embedding_model = "snowflake-arctic-embed-l-v2.0"

            # Create the Cortex Search service
            service_manager.create_cortex_search_service(service_name, table_name, text_column, attributes, warehouse, embedding_model)
            
            # -----------------------------------------------------------
            # Quering Cortex Search Service

            news_search_service = (root
                .databases["MAIN_RAG_DB"]
                .schemas["MAIN_RAG_SCHEMA"]
                .cortex_search_services["news_search"]
            )

            resp = news_search_service.search(
                query=stock_query,
                columns=["TEXT", "AUTHOR","URL","SUMMARY"],
                filter=None,
                limit=1
            )
            print(resp)
            # -----------------------------------------------------------
            # Extracting article text from news search service

            # Instantiate the FieldExtractor class
            field_extractor = FieldExtractor()

            # Converting the extracted text list to a single string .
            textlis = field_extractor.extract_fields(resp)
            context = "".join(textlis) # preprocessing step
            print(context)

            # -----------------------------------------------------------

            # Summarization of news articles
            sentiment_result = analyze_financial_sentiment(context,session=session)
            summary_result = summarize_article(context,session=session)

            # -----------------------------------------------------------

            # Display the summarized key points
            st.subheader("Key Points for Traders")
            st.write(summary_result)

            # Display the sentiment result
            st.subheader("Financial Sentiment Analysis")
            st.write(f"Sentiment: {sentiment_result}")

        else:
            st.warning("No news found for the given query.")

    except Exception as e:
        st.error(f"Error fetching stock news: {e}")

# Option to store results into Snowflake
if st.button("Save Results to Snowflake"):
    try:
        # Process the data and prepare the DataFrame for insertion
        data_to_insert = {
            'Stock Symbol': stock_query,
            'Article Summary': summary_result.result['choices'][0]['text'],
            'Sentiment': sentiment_result.result['choices'][0]['text'],
        }
        results_df = pd.DataFrame([data_to_insert])

        # Insert data into Snowflake (you can customize to specify the table)
        snowflake_inserter = SnowflakeDataInserter(session)
        snowflake_inserter.insert_df(results_df, 'stock_insights_table')

        st.success("Results successfully inserted into Snowflake.")
    except Exception as e:
        st.error(f"Error inserting results into Snowflake: {e}")

# Close the Snowflake session when the app is stopped
if st.button("Close Session"):
    
    snowflake_connector.close_session()
    st.success("Snowflake session closed.")
