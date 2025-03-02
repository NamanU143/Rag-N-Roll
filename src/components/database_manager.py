import logging
from configuration.snowflakeconfig import SnowflakeConnector  # Assuming SnowflakeConnector is the class that manages Snowflake session
from src.exception import CustomException  # Assuming you have a custom exception class

class SnowflakeDatabaseManager:
    """
    Class for managing Snowflake database, schema, and table creation.
    """

    def __init__(self, session):
        self.session = session  # Snowflake session object

    def create_database(self, database_name: str):
        """
        Create a database in Snowflake.

        Args:
            database_name (str): The name of the database to create.
        """
        try:
            # SQL command to create database
            sql = f"CREATE DATABASE IF NOT EXISTS {database_name}"
            self.session.sql(sql).collect()
            logging.info(f"Database {database_name} created successfully (if not already exists).")
        except Exception as e:
            logging.error(f"Error creating database {database_name}: {e}")
            raise CustomException(f"Error creating database {database_name}: {str(e)}")

    def create_schema(self, database_name: str, schema_name: str):
        """
        Create a schema in Snowflake under a specific database.

        Args:
            database_name (str): The name of the database.
            schema_name (str): The name of the schema to create.
        """
        try:
            # SQL command to create schema
            sql = f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name}"
            self.session.sql(sql).collect()
            logging.info(f"Schema {schema_name} created successfully in {database_name}.")
        except Exception as e:
            logging.error(f"Error creating schema {schema_name} in database {database_name}: {e}")
            raise CustomException(f"Error creating schema {schema_name} in database {database_name}: {str(e)}")

    def create_table(self, database_name: str, schema_name: str, table_name: str, columns):
        """
        Create a table in a specified schema in Snowflake.

        Args:
            database_name (str): The name of the database.
            schema_name (str): The name of the schema.
            table_name (str): The name of the table to create.
            columns (dict): A dictionary defining columns and their data types.
        """
        try:
            # Constructing the CREATE TABLE SQL command
            columns_sql = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
            sql = f"""
            CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.{table_name} (
                {columns_sql}
            )
            """
            self.session.sql(sql).collect()
            logging.info(f"Table {table_name} created successfully in {schema_name} schema of {database_name}.")
        except Exception as e:
            logging.error(f"Error creating table {table_name} in schema {schema_name} of {database_name}: {e}")
            raise CustomException(f"Error creating table {table_name} in schema {schema_name} of {database_name}: {str(e)}")
        
    # Function to truncate the table
    def truncate_table(self, database_name: str, schema_name: str, table_name: str):
            """
            Truncate the contents of a table in Snowflake (removes all rows).

            Args:
                database_name (str): The name of the database.
                schema_name (str): The name of the schema.
                table_name (str): The name of the table to truncate.
            """
            try:
                # SQL command to truncate the table
                sql = f"TRUNCATE TABLE {database_name}.{schema_name}.{table_name}"
                self.session.sql(sql).collect()
                logging.info(f"Table {table_name} truncated successfully in {schema_name} schema of {database_name}.")
            except Exception as e:
                logging.error(f"Error truncating table {table_name} in schema {schema_name} of {database_name}: {e}")
                raise CustomException(f"Error truncating table {table_name} in schema {schema_name} of {database_name}: {str(e)}")
