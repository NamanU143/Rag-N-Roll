
#####################################################################################################################
# STREAMLIT #
#####################################################################################################################
import time
import streamlit as st
from snowflake.snowpark import Session
from snowflake.cortex import Complete
from src.configuration.snowflake import SnowflakeConnector

def get_snowflake_session():
    connector = SnowflakeConnector()
    return connector.get_session()

def run_cortex_query(model, query):
    session = get_snowflake_session()
    return Complete(model, query, session=session, stream=True)

st.title("Snowflake Cortex Streamlit App")

model = st.selectbox("Select Model", ["mistral-7b", "llama3.1-70b"])
query = st.text_area("Enter your query", "Which of the clients requires my attention most?")
query = query + "You are Science Teacher who is going to teach to a 4th grade to 8th grade students .Your task is to give some funny,humorous and playful examples for the topics given "
delay = 0.03

if st.button("Run Query"):
    st.write("### Response:")
    response_area = st.empty()
    
    full_response = ""

    for update in run_cortex_query(model, query):
        full_response += update + ""
        response_area.markdown(full_response)
        print(full_response)
        time.sleep(delay)  # Apply delay based on user selection
