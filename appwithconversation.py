import streamlit as st
import pandas as pd
from src.configuration.snowflake import SnowflakeConnector
from src.components.news_extraction import NewsExtractor
from src.components.database_manager import SnowflakeDatabaseManager
from src.components.llm_response_extractor import FieldExtractor
from src.components.create_cortex_search_service import CortexSearchServiceManager
from snowflake.core import Root
from src.components.trade_assist_llm import summarize_article, analyze_financial_sentiment
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

# Streamlit Page Configuration with Financial Theme
st.set_page_config(page_title="Trading Assistant", layout="wide")
st.markdown(
    """
    <style>
        body { background-color: #f5f5f5; }
        .main { background-color: white; border-radius: 10px; padding: 20px; }
        .sidebar .sidebar-content { background-color: #004d00 !important; color: white; }
        h1, h2, h3, h4 { color: #004d00; }
        .stButton>button { background-color: #004d00; color: white; border-radius: 5px; }
    </style>
    """,
    unsafe_allow_html=True
)

st.title("📈 Trading Assistant - Stock Insights")
st.markdown("Get insights on financial market news for informed trading decisions.")

# Initialize session state for conversation history
if "conversation_history" not in st.session_state:
    st.session_state["conversation_history"] = {}

# Sidebar for user inputs and history
st.sidebar.title("💬 Query History")
selected_query = st.sidebar.selectbox("Select a Previous Query:", list(st.session_state["conversation_history"].keys()), index=0 if st.session_state["conversation_history"] else None)

if selected_query:
    st.sidebar.write(f"**Market Sentiment:** {st.session_state['conversation_history'][selected_query]['sentiment']}")
    st.sidebar.markdown("---")
    st.sidebar.write(f"**Market News Summary:** {st.session_state['conversation_history'][selected_query]['summary']}")
    st.sidebar.markdown("---")

st.sidebar.markdown("---")

# User Input Section
st.subheader("🔍 Step 1: Enter Stock Symbol (preferred) or Company Name")
stock_query = st.text_input("Enter Stock Symbol (e.g., TSLA)", "Tesla")

if st.button("Fetch Stock News"):
    try:
        news_fetcher = NewsExtractor(st.secrets["newsapi"]["apikey"], st.secrets["difbot"]["apikey"])
        stock_news_df = news_fetcher.process_news(query=stock_query)
        
        # -----------------------------------------------------------

        # Preprocessing step
        stock_news_df["id"] = stock_news_df.index  # adding index for unique identifier preprocessing step
        stock_news_df.insert(0, "id", stock_news_df.pop("id"))
        stock_news_df.columns = stock_news_df.columns.str.upper()  # preprocessing step
        logging.info("Preprocessing Step Completed !!!!!!!!!!!!!")

        if not stock_news_df.empty:
            st.success("✅ Stock News Fetched Successfully!")
            st.write("### Latest Stock News")
            st.dataframe(stock_news_df.style.set_properties(**{"background-color": "#f0fff0", "color": "#004d00"}))
            
            


            # Store data in Snowflake
            table_name = "NEWSDIFF"
            session.write_pandas(stock_news_df, table_name, auto_create_table=True)
            
            # Cortex Search Service
            service_manager = CortexSearchServiceManager(session)
            service_manager.create_cortex_search_service(
                service_name="news_search",
                table_name=table_name,
                text_column="CONTENT",
                attributes=["ID", "SOURCE", "AUTHOR", "DATE", "TITLE", "URL", "DESCRIPTION"],
                warehouse="COMPUTE_WH",
                embedding_model="snowflake-arctic-embed-l-v2.0"
            )
            
            news_search_service = root.databases["MAIN_RAG_DB"].schemas["MAIN_RAG_SCHEMA"].cortex_search_services["news_search"]
            resp = news_search_service.search(query=stock_query, columns=["DATE", "SOURCE", "CONTENT", "AUTHOR", "URL", "DESCRIPTION"], limit=1)
            
            field_extractor = FieldExtractor()
            context = "".join(field_extractor.extract_fields(resp))
            
            sentiment_result = analyze_financial_sentiment(context, session=session,user_query=stock_query)
            summary_result = summarize_article(context, session=session, user_query=stock_query)
            
            st.subheader("📌 Key Insights")
            st.write(f"{summary_result}") # printing Summary result
            
            st.subheader("📊 Market Sentiment")
            st.write(f"{sentiment_result}") # printing sentiment result
            # Store conversation history
            st.session_state["conversation_history"][stock_query] = {
                "summary": summary_result,
                "sentiment": sentiment_result
            }
            # Creating an instance of SnowflakeDatabaseManager
            db_manager = SnowflakeDatabaseManager(session)
            db_manager.truncate_table(schema_name=snowflake_schema,database_name=snowflake_database,table_name=table_name)
        else:
            st.warning("⚠️ No news found for the given stock symbol.")
    except Exception as e:
        st.error(f"❌ Error: {e}")

# Save Insights 
if st.button("Save Insights"):
    st.rerun()
    st.sucess("Saved Insights Successfully")

# Close Snowflake Session
if st.button("Close Session"):
    session.close()
    st.success("✅ Snowflake session closed.")
