from src.pipeline import Pipeline
import streamlit as st 

pipeline = Pipeline()

query = st.text_input("Enter the Stock Symbol :")

if query:
    st.write(f"You entered: {query}")

newsdf = pipeline.initiate_pipeline(query=query)

if not newsdf.empty:
    st.dataframe(newsdf)