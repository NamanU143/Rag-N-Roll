# import streamlit as st
# import time
# from trade_assist_llm_lang import SnowflakeCortexChat  # Import your chat wrapper class

# # Initialize the chat model
# cortex_chat = SnowflakeCortexChat()

# # Streamlit UI Layout
# st.set_page_config(page_title="TradeRAG: Financial Insights", layout="wide")
# st.title("📊 TradeRAG - Financial Market Insights")

# # User input for stock query
# user_query = st.text_input("Enter stock ticker or financial topic (e.g., 'TSLA', 'Rate Hike'):", "")

# if st.button("Analyze News"):
#     if user_query:
#         st.subheader("🔄 Streaming Response:")
#         output_placeholder = st.empty()  # Create a placeholder for incremental updates

#         streamed_text = ""  # Store the accumulated text

#         # Stream response in real-time with delay
#         for chunk in cortex_chat.chat.stream([
#             {"role": "system", "content": "You are a financial assistant providing structured insights."},
#             {"role": "user", "content": user_query},
#         ]):
#             streamed_text += chunk.content  # Append new chunk
#             output_placeholder.markdown(f"📝 **Response:**\n\n{streamed_text}")  # Update UI dynamically
            
#             time.sleep(0.3)  # Add delay to slow down streaming (Adjust as needed)

#     else:
#         st.warning("⚠️ Please enter a stock ticker or financial topic.")
