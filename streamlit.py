import streamlit as st
import pandas as pd
from src.configuration.snowflake import SnowflakeConnector
# from src.components.news_articles_extraction import StockNewsFetcher
from src.components.news_extraction import NewsExtractor
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

snowflake_username = st.secrets["snowflake"]["username"]
snowflake_password = st.secrets["snowflake"]["password"]
snowflake_account = st.secrets["snowflake"]["account"]
snowflake_role = st.secrets["snowflake"]["role"]
snowflake_warehouse = st.secrets["snowflake"]["warehouse"]
snowflake_database = st.secrets["snowflake"]["database"]
snowflake_schema = st.secrets["snowflake"]["schema"]
newsapikey = st.secrets["newsapi"]["apikey"]
difbot = st.secrets["difbot"]["apikey"]

logging.info("<< Initializing Snowflake Session")

snowflake_connector = SnowflakeConnector(
    snowflake_username=snowflake_username,
    snowflake_account=snowflake_account,
    snowflake_password=snowflake_password,
    snowflake_role=snowflake_role,
    snowflake_warehouse=snowflake_warehouse,
    snowflake_database=snowflake_database,
    snowflake_schema=snowflake_schema
)

session = snowflake_connector.get_session()
root = Root(session)

logging.info("Snowflake Session Established >> ")

# Set Streamlit page configuration
st.set_page_config(page_title="Trading Assistant", layout="wide")

# Title and description
st.title("Trading Assistant - Stock Insights")
st.markdown("""
This app helps traders by providing insights based on stock-related news articles. Enter a stock symbol to get the latest news and analysis.
""")

# Define the news query input
st.subheader("Step 1: Enter Stock Symbol or Company")
stock_query = st.text_input("Enter Stock Symbol or Company (e.g., Tesla)", "Tesla")

# Create a button to fetch stock news
if st.button("Fetch Stock News"):
    try:
        # -----------------------------------------------------------

        # Fetching stock news
        news_fetcher = NewsExtractor(newsapikey,difbot)
        stock_news_df = news_fetcher.process_news(query=stock_query)

        # -----------------------------------------------------------

        # Preprocessing step
        stock_news_df["id"] = stock_news_df.index  # adding index for unique identifier preprocessing step
        stock_news_df.insert(0, "id", stock_news_df.pop("id"))
        stock_news_df.columns = stock_news_df.columns.str.upper()  # preprocessing step
        logging.info("Preprocessing Step Completed !!!!!!!!!!!!!")

        if not stock_news_df.empty:
            # -----------------------------------------------------------
            st.write("### Fetched Stock News:")
            st.dataframe(stock_news_df)         # dev step

            # -----------------------------------------------------------
            # Creating an instance of SnowflakeDatabaseManager
            # db_manager = SnowflakeDatabaseManager(session)

            # # Step 1: Create database
            # db_name = "MAIN_RAG_DB"
            # db_manager.create_database(db_name)
            # logging.info(f"Created Database : {db_name} Step ")

    # # -----------------------------------------------------------     # this piece of code is used for standard structure of db,schema and table which can be used 
                                                                        # for multiple apis with different response structures and field names.
                                                                        # Which data to format in standard structure : rss feeds, websocket data,apis and official dfs.
                                                                    
            # # Step 2: Create schema
            # schema_name = "MAIN_RAG_SCHEMA"
            # db_manager.create_schema(db_name, schema_name)
            # logging.info(f"Created Schema : {schema_name} Step ")


            # # -----------------------------------------------------------

            # # Step 3: Create table

            # # Mapping dataframe columns to snowflake data types
            # data_type_mapper = SnowflakeDataTypeMapper()
            # columns = data_type_mapper.get_column_data_types(stock_news_df)
            # table_name = "MY_NEW_TABLE"
            # db_manager.create_table(db_name, schema_name, table_name, columns)
            # logging.info(f"Created Table : {table_name} Step ")

            # # -----------------------------------------------------------
            # Step 4: Insert data into snowflake table
            table_name = "NEWSDIFF"  # Specify your table name here
            snowpark_df = session.write_pandas(stock_news_df, table_name, auto_create_table=True)
            logging.info(f"<<< Inserted dataframe into snowflake :\n {snowpark_df}>>>")
            # -----------------------------------------------------------

            service_manager = CortexSearchServiceManager(session)

            # Define parameters for creating the Cortex Search service
            service_name = "news_search"
            table_name = "NEWSDIFF"
            text_column = "CONTENT"
            attributes = ["ID","SOURCE","AUTHOR", "DATE", "TITLE", "URL","DESCRIPTION"]
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
                columns=["DATE","SOURCE","CONTENT", "AUTHOR", "URL", "DESCRIPTION"],
                filter=None,
                limit=1
            )

            # -----------------------------------------------------------
            # Extracting article text from news search service
            field_extractor = FieldExtractor()
            textlis = field_extractor.extract_fields(resp)
            context = "".join(textlis)

            # -----------------------------------------------------------
            # AI insights from news articles
            sentiment_result = analyze_financial_sentiment(context, session=session)
            summary_result = summarize_article(context, session=session,query=stock_query)

            # -----------------------------------------------------------
            # Display the summarized key points
            st.subheader("Summarized Key Points")
            st.write(summary_result)

            # Display the sentiment result
            st.subheader("Sentiment of Market based on given stock/asset")
            st.write(f"Sentiment: {sentiment_result}")

            # Creating an instance of SnowflakeDatabaseManager
            # db_manager = SnowflakeDatabaseManager(session)
            # db_manager.truncate_table(schema_name=snowflake_schema,database_name=snowflake_database,table_name=table_name)

        else:
            st.warning("No news found for the given query.")

    except Exception as e:
        st.error(f"Error fetching stock news: {e}")

# # Option to store results into Snowflake
# if st.button("Save Results to Snowflake"):
#     try:
#         # Process the data and prepare the DataFrame for insertion
#         data_to_insert = {
#             'Stock Symbol': stock_query,
#             'Article Summary': summary_result,
#             'Sentiment': sentiment_result,
#         }
#         results_df = pd.DataFrame([data_to_insert])

#         # Insert data into Snowflake (you can customize to specify the table)
#         table_name = "STOCK_INSIGHT"  # Specify your table name here
#         stockinsights = session.write_pandas(results_df, table_name, auto_create_table=True)
#         logging.info(f"<<< Inserted dataframe into snowflake :\n {stockinsights}>>>")

#         st.success("Results successfully inserted into Snowflake.")
#     except Exception as e:
#         st.error(f"Error inserting results into Snowflake: {e}")

# Display conversation history in the sidebar
if "conversation_history" in st.session_state and st.session_state["conversation_history"]:
    st.sidebar.title("Conversation History")
    for idx, conversation in enumerate(st.session_state["conversation_history"]):
        st.sidebar.subheader(f"Conversation {idx + 1}: {conversation['Stock Symbol']}")
        st.sidebar.write(f"**Summary:** {conversation['Article Summary']}")
        st.sidebar.write(f"**Sentiment:** {conversation['Sentiment']}")
        st.sidebar.markdown("---")

# Close the Snowflake session when the app is stopped
if st.button("Close Session"):
    snowflake_connector.close_session()
    st.success("Snowflake session closed.")
