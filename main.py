import streamlit as st
from src.pipeline import Pipeline
from src.logger import logging
from src.components.ai_agents import summarize
from src.components.trade_assist_llm_sql import summarize_article

st.title("AI DRIVEN MARKET ANALYSIS FOR TRADERS")

rad = st.sidebar.title("Conversation History")


# first,second = st.columns([1,1])

query = st.text_input("Enter the stock symbol")
focus_on = st.text_input("What topic you want to focus on while generating AI Analysis")

if query and focus_on :

    pipeline = Pipeline()
    news_article = pipeline.initiate_news_extraction_pipeline(query=query)

    if news_article:
        st.markdown(news_article)
    if news_article :
        st.write_stream(summarize_article(query=query,article_text=news_article,focus_topic=focus_on))
    