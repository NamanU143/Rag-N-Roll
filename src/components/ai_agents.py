from src.logger import logging
import time
from langchain_community.chat_models import ChatSnowflakeCortex
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from configuration.snowflakeconfig import SnowflakeConnector


class SnowflakeCortexChat:
    """Wrapper class to manage Snowflake Cortex LLM interactions for financial insights."""

    def __init__(self, model="mistral-large2", temperature=0.7, top_p=0.95):
        self.session = self.get_snowflake_session()
        self.chat = ChatSnowflakeCortex(
            model=model,
            cortex_function="complete",
            snowflake_database="MAIN_RAG_DB",
            snowflake_schema="MAIN_RAG_SCHEMA",
            temperature=temperature,
            top_p=top_p,
            session=self.session
        )
        self.conversation_history = []  # Store conversation history

    def get_snowflake_session(self):
        """Retrieves Snowflake session."""
        try:
            connector = SnowflakeConnector()
            return connector.get_session()
        except Exception as e:
            logging.error(f"Failed to establish Snowflake session: {e}")
            raise

    def summarize_article(self, article_text, user_query):
        """Summarizes financial news with structured insights (streaming enabled)."""
        prompt = f"""
        You are a financial expert. Summarize the following article focusing on {user_query}. Extract key financial and trading insights.

        **Article:**
        {article_text}

        **Summary:**
        - **Stock Performance & Market Reaction:** (Mention price movements, investor sentiment, and market impact.)
        - **Key Financial Metrics:** (Earnings, revenue, debt, P/E ratio, etc.)
        - **Industry Trends & Macro Factors:** (Regulatory impact, sector trends.)
        - **Company-Specific News:** (Mergers, leadership changes, product launches.)
        - **Sentiment Analysis:** (Classify sentiment as Bullish, Bearish, or Neutral.)
        """
        return self._execute_cortex_query_stream(prompt)

    def analyze_financial_sentiment(self, article_text):
        """Performs sentiment analysis on financial news (streaming enabled)."""
        prompt = f"""
        You are a financial sentiment analysis AI. Analyze the sentiment of the given article.

        **Article:**
        "{article_text}"

        **Response Format:**
        - **Sentiment:** [Positive / Slightly Positive / Neutral / Slightly Negative / Negative]
        - **Sentiment Direction:** [Positive to Neutral / Neutral to Positive / Negative to Neutral, etc.]
        - **Reasoning:** (Provide 2-4 key points explaining sentiment.)
        """
        return self._execute_cortex_query_stream(prompt)

    def _execute_cortex_query_stream(self, prompt):
        """Streams response from Cortex with a controlled delay."""
        sql_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large2',
            ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT('role', 'user', 'content', {repr(prompt)})
            ),
            OBJECT_CONSTRUCT(
                'temperature', 0.7,
                'max_tokens', 5000,
                'stream', TRUE
            )
        )
        """
        try:
            streamed_text = ""
            for chunk in self.session.sql(sql_query).collect():
                text_chunk = chunk[0] if chunk else ""
                streamed_text += text_chunk
                print(text_chunk, end="", flush=True)
                time.sleep(0.1)  # Slow down streaming
            return streamed_text
        except Exception as e:
            logging.error(f"Streaming error: {e}")
            return "Streaming error occurred."

    def chat_with_cortex(self, user_input):
        """Handles conversation with Cortex LLM while maintaining conversation history."""
        self.conversation_history.append(HumanMessage(content=user_input))
        messages = [
            SystemMessage(content="You are a financial assistant providing structured insights.")
        ] + self.conversation_history
        try:
            response = self.chat.invoke(messages)
            self.conversation_history.append(AIMessage(content=response.content))
            return response.content
        except Exception as e:
            logging.error(f"Error during chat completion: {e}")
            return "An error occurred while processing your request."

    # def stream_response(self, user_input):
    #     """Streams response from Cortex while maintaining conversation history."""
    #     self.conversation_history.append(HumanMessage(content=user_input))
    #     messages = [
    #         SystemMessage(content="You are a financial assistant providing structured insights.")
    #     ] + self.conversation_history
    #     try:
    #         streamed_text = ""
    #         for chunk in self.chat.stream(messages):
    #             text_chunk = chunk.content if hasattr(chunk, "content") else str(chunk)
    #             streamed_text += text_chunk
    #             print(text_chunk, end="", flush=True)
    #             time.sleep(0.1)  # Slow down streaming
    #         self.conversation_history.append(AIMessage(content=streamed_text))
    #         return streamed_text
    #     except Exception as e:
    #         logging.error(f"Streaming error: {e}")
    #         return "Streaming error occurred."

# Example usage
# if __name__ == "__main__":
#     cortex_chat = SnowflakeCortexChat()
    
#     # Test Summarization
#     article = "Tesla stock surged after strong earnings report and new product announcements."
#     summary = cortex_chat.summarize_article(article, "Tesla stock")
#     print("\n🔹 **Summary:**\n", summary)

#     # Test Sentiment Analysis
#     sentiment = cortex_chat.analyze_financial_sentiment(article, user_query="Tesla stock")
#     print("\n🔹 **Sentiment Analysis:**\n", sentiment)

#     # Test Chat History
#     print("\n🔹 **Conversation History Test:**")
#     print(cortex_chat.chat_with_cortex("What is the impact of Tesla's earnings on the stock market?"))
#     print(cortex_chat.chat_with_cortex("How does this compare to last quarter?"))

#     # Test Streaming
#     print("\n🔹 **Streaming Response:**")
#     cortex_chat.stream_response("Explain AI in simple terms.")
