import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from src.logger import logging
from src.exception import CustomException
import streamlit as st

class SnowflakeInserter:
    def __init__(self):
        try:
            snowflake_username = st.secrets["snowflake"]["username"]
            snowflake_password = st.secrets["snowflake"]["password"]
            snowflake_account = st.secrets["snowflake"]["account"]
            snowflake_warehouse = st.secrets["snowflake"]["warehouse"]

            self.conn = snowflake.connector.connect(
                user=snowflake_username,
                password=snowflake_password,
                account=snowflake_account,
                warehouse=snowflake_warehouse
            )
            self.cursor = self.conn.cursor()
            
            # Verify if database and schema exist
            self.cursor.execute("SHOW DATABASES LIKE 'MAIN_RAG_DB'")
            if not self.cursor.fetchall():
                raise CustomException("Database MAIN_RAG_DB does not exist.")
            
            self.cursor.execute("SHOW SCHEMAS IN DATABASE MAIN_RAG_DB")
            schemas = [row[1] for row in self.cursor.fetchall()]
            if "MAIN_RAG_SCHEMA" not in schemas:
                raise CustomException("Schema MAIN_RAG_SCHEMA does not exist.")
            
            # Explicitly setting database and schema
            self.cursor.execute("USE DATABASE MAIN_RAG_DB")
            self.cursor.execute("USE SCHEMA MAIN_RAG_SCHEMA")
            
            logging.info("Successfully connected to Snowflake and set database/schema.")
        except Exception as e:
            logging.critical(f"Failed to connect to Snowflake: {CustomException(e)}", exc_info=True)
            raise CustomException(e)

    def insert_dataframe(self, df: pd.DataFrame):
        """Inserts the given DataFrame into Snowflake."""
        try:
            if df.empty:
                logging.warning("Received an empty DataFrame, skipping insertion.")
                return

            logging.info(f"Starting insertion of {len(df)} records into Snowflake.")
            
            # Generate table schema dynamically
            df.drop(columns=["ID"],inplace=True)
            column_defs = ", ".join([f"{col} STRING" for col in df.columns])
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS MY_NEW_TABLE (
                    ID INT AUTOINCREMENT PRIMARY KEY,
                    {column_defs}
                )
            """
            
            # Ensure table exists before insertion
            self.cursor.execute(create_table_sql)
            
            success, num_chunks, num_rows, _ = write_pandas(
                self.conn, df, table_name="MY_NEW_TABLE", auto_create_table=False
            )
            
            self.conn.commit()  # Commit transaction
            
            if success:
                logging.info(f"Successfully inserted {num_rows} records into MY_NEW_TABLE in {num_chunks} chunks.")
            else:
                logging.error("Failed to insert records into Snowflake.")

        except Exception as e:
            logging.error(f"Error inserting DataFrame into Snowflake: {CustomException(e)}", exc_info=True)
        
    def close_connection(self):
        """Closes the Snowflake connection."""
        try:
            self.cursor.close()
            self.conn.close()
            logging.info("Closed Snowflake connection successfully.")
        except Exception as e:
            logging.warning(f"Error closing Snowflake connection: {CustomException(e)}", exc_info=True)