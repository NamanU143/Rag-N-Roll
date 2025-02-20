import logging
import pandas as pd
from src.configuration.snowflake import SnowflakeConnector  # Assuming SnowflakeConnector is the class that manages Snowflake session
from src.exception import CustomException  # Assuming you have a custom exception class
from src.components.data_mapper import SnowflakeDataTypeMapper
from src.logger import setup_logger
from src.constants.snowflakedatacreds import DATABASENAME,SCHEMA_NAME,TABLE_NAME

setup_logger()

class SnowflakeDatabaseManager:
    """
    Class for managing Snowflake database, schema, and table creation.
    """

    def __init__(self, session):
        self.session = session  # Snowflake session object
        self.database_name = DATABASENAME
        self.schema_name = SCHEMA_NAME
        self.table_name = TABLE_NAME

    def create_database(self):
        """
        Create a database in Snowflake.

        Args:
            database_name (str): The name of the database to create.
        """
        try:
            # SQL command to create database
            sql = f"CREATE DATABASE IF NOT EXISTS {self.database_name}"
            self.session.sql(sql).collect()
            logging.info(f"Database {self.database_name} created successfully (if not already exists).")
        except Exception as e:
            logging.error(f"Error creating database {self.database_name}: {e}")
            raise CustomException(f"Error creating database {self.database_name}: {str(e)}")

    def create_schema(self):
        """
        Create a schema in Snowflake under a specific database.

        Args:
            database_name (str): The name of the database.
            schema_name (str): The name of the schema to create.
        """
        try:
            # SQL command to create schema
            sql = f"CREATE SCHEMA IF NOT EXISTS {self.database_name}.{self.schema_name}"
            self.session.sql(sql).collect()
            logging.info(f"Schema {self.schema_name} created successfully in {self.database_name}.")
        except Exception as e:
            logging.error(f"Error creating schema {self.schema_name} in database {self.database_name}: {e}")
            raise CustomException(f"Error creating schema {self.schema_name} in database {self.database_name}: {str(e)}")

    def create_table(self,df:pd.Dataframe):
        """
        Create a table in a specified schema in Snowflake.

        Args:
            database_name (str): The name of the database.
            schema_name (str): The name of the schema.
            table_name (str): The name of the table to create.
            columns (dict): A dictionary defining columns and their data types.
        """
        try:

            ###
            table_name = "STOCK_NEWS"
            snowflake_data_mapper = SnowflakeDataTypeMapper()
            columns = snowflake_data_mapper.get_column_data_types(dataframe=df)
            
            ###
            
            # Constructing the CREATE TABLE SQL command
            columns_sql = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
            sql = f"""
            CREATE TABLE IF NOT EXISTS {self.database_name}.{self.schema_name}.{self.table_name} (
                {columns_sql}
            )
            """
            self.session.sql(sql).collect()
            logging.info(f"Table {self.table_name} created successfully in {self.schema_name} schema of {self.database_name}.")
        except Exception as e:
            logging.error(f"Error creating table {self.table_name} in schema {self.schema_name} of {self.database_name}: {e}")
            raise CustomException(f"Error creating table {self.table_name} in schema {self.schema_name} of {self.database_name}: {str(e)}")
        
    # Function to truncate the table
    def truncate_table(self):
            """
            Truncate the contents of a table in Snowflake (removes all rows).

            Args:
                database_name (str): The name of the database.
                schema_name (str): The name of the schema.
                table_name (str): The name of the table to truncate.
            """
            try:
                # SQL command to truncate the table
                sql = f"TRUNCATE TABLE {self.database_name}.{self.schema_name}.{self.table_name}"
                self.session.sql(sql).collect()
                logging.info(f"Table {self.table_name} truncated successfully in {self.schema_name} schema of {self.database_name}.")
            except Exception as e:
                logging.error(f"Error truncating table {self.table_name} in schema {self.schema_name} of {self.database_name}: {e}")
                raise CustomException(f"Error truncating table {self.table_name} in schema {self.schema_name} of {self.database_name}: {str(e)}")


    def initiate_snowflakedbmanager(self):
        try:
            logging.info(f"Creating Database {self.database_name}")
            self.create_database
            logging.info(f"Creating Database {self.schema_name}")
            self.create_schema
            logging.info(f"Creating Database {self.table_name}")
            self.create_table

        except Exception as e :
            raise CustomException(e)