import streamlit as st
import pandas as pd
from src.configuration.snowflake import SnowflakeConnector
from src.components.news_extraction import NewsExtractor
from src.components.database_manager import SnowflakeDatabaseManager
from components.cortex_response_extractor import FieldExtractor
from src.components.create_cortex_search_service import CortexSearchServiceManager
from snowflake.core import Root
from components.trade_assist_llm_sql import summarize_article, analyze_financial_sentiment
from src.logger import setup_logger
import logging

# Setting up custom logger
setup_logger()

# Initialize Snowflake session
# snowflake_username = st.secrets["snowflake"]["username"]
# snowflake_password = st.secrets["snowflake"]["password"]
# snowflake_account = st.secrets["snowflake"]["account"]
# snowflake_role = st.secrets["snowflake"]["role"]
# snowflake_warehouse = st.secrets["snowflake"]["warehouse"]
# snowflake_database = st.secrets["snowflake"]["database"]
# snowflake_schema = st.secrets["snowflake"]["schema"]
# newsapikey = st.secrets["newsapi"]["apikey"]
# difbot = st.secrets["difbot"]["apikey"]

logging.info("<< Initializing Snowflake Session")

snowflake_connector = SnowflakeConnector(
    # # snowflake_username=snowflake_username,
    # # snowflake_account=snowflake_account,
    # snowflake_password=snowflake_password,
    # snowflake_role=snowflake_role,
    # snowflake_warehouse=snowflake_warehouse,
    # snowflake_database=snowflake_database,
    # snowflake_schema=snowflake_schema
)

session = snowflake_connector.get_session()
root = Root(session)

logging.info("Snowflake Session Established >>")

# Streamlit Page Configuration
st.set_page_config(page_title="Trading Assistant", layout="wide", page_icon="📈")
st.markdown(
    """
    <style>
        body { background-color: #0A0A0A; color: #E0E0E0; }
        .main { background-color: #121212; border-radius: 10px; padding: 20px; }
        .sidebar .sidebar-content { background-color: #222222 !important; color: #E0E0E0; border-right: 1px solid #444; }
        h1, h2, h3, h4 { color: #00B8D9; }
        .stButton>button { background-color: #00B8D9; color: white; border-radius: 5px; padding: 10px 24px; border: none; }
        .stButton>button:hover { background-color: #008C99; }
        .stTextInput>div>div>input { background-color: #333; color: #E0E0E0; border: 1px solid #444; }
        .stDataFrame { background-color: #1E1E1E; color: #E0E0E0; border: 1px solid #444; }
        .block-container { padding: 20px; }
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown(
    """
    <h2 style='text-align: center; color: #E0E0E0;'>📈 AI DRIVEN MARKET ANALYSIS FOR TRADERS </h2>
    """,
    unsafe_allow_html=True
)

# Initialize session state for conversation history
if "conversation_history" not in st.session_state:
    st.session_state["conversation_history"] = {}

# Sidebar for user inputs and history
st.sidebar.markdown(
    "<h1 style='color: white;'>💬 Query History</h1>",
    unsafe_allow_html=True
)
selected_query = st.sidebar.selectbox("Select a Previous Query:", list(st.session_state["conversation_history"].keys()), index=0 if st.session_state["conversation_history"] else None)

if selected_query:
    st.sidebar.write(f"Market Sentiment: {st.session_state['conversation_history'][selected_query]['sentiment']}")
    st.sidebar.markdown("---")
    st.sidebar.write(f"Market News Summary: {st.session_state['conversation_history'][selected_query]['summary']}")
    st.sidebar.markdown("---")

st.sidebar.markdown("---")
# User Input Section
st.markdown(
    "<h3 style='color: White;'> Enter Stock Name or Company Name</h3>",
    unsafe_allow_html=True
)

stock_query = st.text_input("( Example : TSLA or Tesla )")

# Add custom CSS for button styling
st.markdown(
    """
    <style>
        .stButton>button {
            font-size: 18px;
            background-color: #008C99;
            color: white;
            border: 1px solid white;
            border-radius: 5px;
            padding: 10px 24px;
            width: 100%;
        }
        .stButton>button:hover {
            background-color: #008C99;
            color: white;
            border: 1px solid white;
        }
    </style>
    """,
    unsafe_allow_html=True
)

# Create two columns with zero spacing
col1, col2 = st.columns([1, 1], gap="small")

# Fetch Stock News Button
with col1:
    if st.button("🚀 Fetch Stock News", use_container_width=True):
        try:
            news_fetcher = NewsExtractor()
            stock_news_df = news_fetcher.process_news(query=stock_query)

            stock_news_df["id"] = stock_news_df.index
            stock_news_df.insert(0, "id", stock_news_df.pop("id"))
            stock_news_df.columns = stock_news_df.columns.str.upper()
            logging.info("Preprocessing Step Completed")

            if not stock_news_df.empty:
                table_name = "NEWSDIFF"
                session.write_pandas(stock_news_df, table_name, auto_create_table=True)
                
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
                # Add custom CSS for cards and layout (Updated for vertical card layout)
                st.markdown(
                    """
                    <style>
                    /* Card Style */
                    .card {
                        background-color: #333;
                        border-radius: 10px;
                        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
                        padding: 20px;
                        margin: 10px 0;  /* Adjust margin for vertical layout */
                        transition: transform 0.3s ease;
                        color: #E0E0E0;
                        font-size: 16px;
                        width: 100%; /* Full width for vertical layout */
                    }
                    .card:hover {
                        transform: scale(1.05);
                        box-shadow: 0 8px 16px rgba(0, 0, 0, 0.3);
                    }
                    .card h3 {
                        color: #00B8D9;
                        font-size: 20px;
                        margin-bottom: 15px;
                    }
                    .card p {
                        color: #D3D3D3;
                        font-size: 14px;
                        line-height: 1.6;
                    }
                    .card a {
                        color: #00B8D9;
                    }
                    .container {
                        display: flex;
                        flex-direction: column; /* Stack cards vertically */
                        gap: 20px; /* Add space between cards */
                    }
                </style>

                    """,
                    unsafe_allow_html=True
                )

                # Create a container for cards and render them
                st.markdown(
                    """
                    <div class="container">
                    """, unsafe_allow_html=True
                )

                for _, row in stock_news_df.iterrows():
                    st.markdown(
                        f"""
                        <div class="card">
                            <h3>{row['TITLE']}</h3>
                            <p><strong>Source:</strong> {row['SOURCE']}</p>
                            <p><strong>Author:</strong> {row['AUTHOR']}</p>
                            <p><strong>Date:</strong> {row['DATE']}</p>
                            <p>{row['DESCRIPTION']}</p>
                            <a href="{row['URL']}" target="_blank">Read More</a>
                        </div>
                        """, unsafe_allow_html=True
                    )

                st.markdown(
                    """
                    </div>
                    """, unsafe_allow_html=True
                )

                # Apply custom CSS for headings and results styling
                st.markdown(
                    """
                    <style>
                        .key-insight, .sentiment-result {
                            background-color: #333333;  /* Dark background for results */
                            border: 1px solid #444444;  /* Subtle darker grey border */
                            padding: 15px;
                            margin-top: 10px;
                            border-radius: 8px;
                            color: #E0E0E0;  /* Light grey text for results */
                            font-size: 16px;
                        }
                        .key-insight p, .sentiment-result p {
                            font-size: 18px;
                            color: #D3D3D3;  /* Light grey for paragraph text */
                        }
                        .key-insight h3, .sentiment-result h3 {
                            font-size: 22px;
                            color: #00B8D9;  /* Cyan color for subheadings for emphasis */
                        }
                    </style>
                    """, unsafe_allow_html=True
                )
                # The rest of the code continues as before...
                # Cortext Search Service, News Search, Sentiment Analysis, etc.
                # Display Key Insights and Sentiment Analysis
                # st.subheader("📌 Key Insights", anchor="key-insight")
                st.markdown(f'<div class="key-insight"><p>{summary_result}</p></div>', unsafe_allow_html=True)

                st.subheader("📊 Market Sentiment", anchor="sentiment-result")
                st.markdown(f'<div class="sentiment-result"><h3>Sentiment Analysis:</h3><p>{sentiment_result}</p></div>', unsafe_allow_html=True)

                # Store the conversation history
                st.session_state["conversation_history"][stock_query] = {
                    "summary": summary_result,
                    "sentiment": sentiment_result
                }

                db_manager = SnowflakeDatabaseManager(session)
                db_manager.truncate_table(schema_name=session.get_current_schema(), database_name=session.get_current_database(), table_name=table_name)
            else:
                st.warning("⚠ No news found for the given stock symbol.")
        except Exception as e:
            st.error(f"❌ Error: {e}")

# Save Insights Button
with col2:
    if st.button("💾 Save Insights", use_container_width=True):
        st.rerun()
        st.success("✅ Insights Saved Successfully!")