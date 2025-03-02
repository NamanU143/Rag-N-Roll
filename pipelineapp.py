from src.pipeline import Pipeline
import streamlit as st 
from components.ai_agents import SnowflakeCortexChat

pipeline = Pipeline()

query = st.text_input("Enter the Stock Symbol :")

if query:
    st.write(f"You entered: {query}")

resp = pipeline.initiate_pipeline(query=query)

model = st.selectbox("Select Model", ["mistral-7b", "llama3.1-70b"])

article = resp
if resp != None :
    chatwithai = SnowflakeCortexChat(model=model)

    summary = chatwithai.summarize_article(article, "Tesla stock")
    print("\n🔹 **Summary:**\n", summary)

    # Test Sentiment Analysis
    sentiment = chatwithai.analyze_financial_sentiment(article, user_query="Tesla stock")
    print("\n🔹 **Sentiment Analysis:**\n", sentiment)

    # Test Chat History
    print("\n🔹 **Conversation History Test:**")
    print(chatwithai.chat_with_cortex("What is the impact of Tesla's earnings on the stock market?"))
    print(chatwithai.chat_with_cortex("How does this compare to last quarter?"))