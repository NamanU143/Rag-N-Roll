# Snowflake Connection Code
import streamlit

from src.configuration.snowflake import SnowflakeConnector

# News Extraction Code

from src.components.news_extraction import NewsExtractor

newsextraction = NewsExtractor()
df = newsextraction.process_news(query="NVIDIA")


from src.components.preprocess_newsdf import PreprocessNewsdf
preprocess = PreprocessNewsdf(newsdf=df)
newsdf = preprocess.process_newsdf()


snowflake_connector = SnowflakeConnector()
session = snowflake_connector.get_session()
print("session sucessfull")

print(session.get_current_role()) 
print(session.get_current_user())
print(session.get_current_database())
print(session.get_current_schema())


# This class is responsible for creating the database, schema, and table in snowflake .
from src.components.database_manager import SnowflakeDatabaseManager
from src.components.format_df import SnowflakeDataTypeMapper


# -----------------------------------------------------------
# Creating an instance of SnowflakeDatabaseManager
db_manager = SnowflakeDatabaseManager(session)

# Step 1: Create database
db_name = "MAIN_RAG_DB"
db_manager.create_database(db_name)

# -----------------------------------------------------------
# Step 2: Create schema
schema_name = "MAIN_RAG_SCHEMA"
db_manager.create_schema(db_name, schema_name)
# -----------------------------------------------------------


# Mapping dataframe columns to snowflake data types

# Initialize the class
data_type_mapper = SnowflakeDataTypeMapper()

# Map the DataFrame columns to Snowflake-compatible dafta types
columns = data_type_mapper.get_column_data_types(newsdf)

# Print or use the resulting column data types
print(columns)
# -----------------------------------------------------------

# Step 3: Create table
table_name = "MY_NEW_TABLE"
db_manager.create_table(db_name, schema_name, table_name, columns)