# # #####################################################################################################################
# # # STREAMLIT #
# # #####################################################################################################################
# import time
# import streamlit as st
# from snowflake.snowpark import Session
# from snowflake.cortex import Complete
# from src.configuration.snowflake import SnowflakeConnector
# from src.components.ai_agents import run_cortex_query


# st.title("Snowflake Cortex Streamlit App")

# model = st.selectbox("Select Model", ["mistral-7b", "llama3.1-70b"])
# # query = st.text_area("Enter your query", "Which of the clients requires my attention most?")
# # query = query + "You are Science Teacher who is going to teach to a 4th grade to 8th grade students .Your task is to give some funny,humorous and playful examples for the topics given "
# delay = 0.03

# if st.button("Run Query"):
#     st.write("### Response:")
#     response_area = st.empty()
    
#     full_response = ""

#     for update in run_cortex_query(model):
#         full_response += update + ""
#         response_area.markdown(full_response)
#         print(full_response)
#         time.sleep(delay)  # Apply delay based on user selection


# ########################################################################################################################################
# ########################################################################################################################################

# # from langchain_community.chat_models import ChatSnowflakeCortex
# # from langchain_core.messages import HumanMessage, SystemMessage
# # from src.configuration.snowflake import SnowflakeConnector
# # import time


# # conn = SnowflakeConnector()
# # session = conn.get_session()

# # chat = ChatSnowflakeCortex(
# #     # Change the default cortex model and function
# #     model="mistral-large",
# #     cortex_function="complete",

# #     # Change the default generation parameters
# #     temperature=0,
# #     max_tokens=1024,
# #     top_p=0.95,

# #     session=session,
# #     snowflake_database="MAIN_RAG_DB",
# #     snowflake_schema="MAIN_RAG_SCHEMA"
# # )

# # article_text = """ Tesla's Stock Surges After Strong Q4 Earnings Report
# # Tesla Inc. (TSLA) saw a 7% surge in its stock price after reporting better-than-expected Q4 earnings on Wednesday. The company posted a net income of $3.9 billion, exceeding Wall Street estimates. Revenue rose 15% year-over-year to $25.2 billion, driven by record vehicle deliveries and strong demand for its Model Y and Model 3.
# # CEO Elon Musk stated that Tesla is on track to produce 2 million vehicles in 2025, with increased production capacity in Gigafactories across Texas and Berlin. However, the company warned of macroeconomic challenges, including rising interest rates affecting vehicle affordability.
# # Despite concerns, analysts remain optimistic, with Morgan Stanley upgrading Tesla's stock outlook from Neutral to Overweight, citing strong demand for EVs and Tesla's advancements in AI-driven self-driving technology.
# # """
# # article_text = article_text.replace("'", "''")

# # focus_topic = "Tesla stock performance"

# # prompt = f'''
# #     You are a financial expert. Summarize the following article focusing on {focus_topic}. Extract key financial and trading insights.

# #     **Article:**
# #     {article_text}

# #     **Summary:**
# #     - **Stock Performance & Market Reaction:** (Price movements, investor sentiment, and market impact.)
# #     - **Key Financial Metrics:** (Earnings, revenue, debt, P/E ratio, etc.)
# #     - **Industry Trends & Macro Factors:** (Regulatory impact, sector trends.)
# #     - **Company-Specific News:** (Mergers, leadership changes, product launches.)
# #     - **Sentiment Analysis:** (Classify sentiment as Bullish, Bearish, or Neutral.)
# #     '''

# # messages = [
# #     SystemMessage(content="You are a financial assistant providing structured insights."),
# #     HumanMessage(content=prompt),
# # ]

# # # Invoke the stream method and print each chunk as it arrives
# # print("Stream Method Response:")
# # for chunk in chat.stream(messages):
# #     time.sleep(0.3)
# #     print(chunk.message.content)

