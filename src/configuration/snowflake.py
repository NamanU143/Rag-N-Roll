import os
import logging
from dotenv import load_dotenv
from snowflake.snowpark import Session
from snowflake.core import Root
from src.exception import CustomException  # Assuming you have a CustomException class for handling exgceptions
import streamlit as st

class SnowflakeConnector:
    """
    A class to handle the connection to Snowflake and manage the session.
    """

    def __init__(self,snowflake_username,snowflake_password,snowflake_account,snowflake_role,snowflake_warehouse,snowflake_database,snowflake_schema):
        try:
            # load_dotenv()

            # self.connection_parameters = {
            #     "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            #     "user": os.getenv("SNOWFLAKE_USERNAME"),
            #     "password": os.getenv("SNOWFLAKE_PASSWORD"),
            #     "role": os.getenv("SNOWFLAKE_ROLE"),
            #     "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            #     "database": os.getenv("SNOWFLAKE_DATABASE"),
            #     "schema": os.getenv("SNOWFLAKE_SCHEMA")
            # }



            self.connection_parameters = {
                "account": snowflake_account,
                "user": snowflake_username,
                "password": snowflake_password,
                "role": snowflake_role,
                "warehouse": snowflake_warehouse,
                "database": snowflake_database,
                "schema": snowflake_schema
            }


            logging.info("Successfully loaded connection parameters from .env file.")
            
            self.session = Session.builder.configs(self.connection_parameters).create()
            self.root = Root(self.session)
            logging.info("Snowflake session created successfully.")
        
        except Exception as e:
            logging.error(f"Error initializing Snowflake session: {str(e)}")
            raise CustomException(f"Error initializing Snowflake session",{str(e)})

    def get_session(self):
        """
        Returns the Snowflake session object.
        """
        try:
            print("Returning the Snowflake session object.")
            return self.session
        except Exception as e:
            logging.error(f"Error retrieving Snowflake session: {str(e)}")
            raise CustomException(f"Error retrieving Snowflake session: {str(e)}")

    def close_session(self):
        """
        Closes the Snowflake session.
        """
        try:
            logging.info("Closing the Snowflake session.")
            self.session.close()
            logging.info("Snowflake session closed successfully.")
        except Exception as e:
            logging.error(f"Error closing Snowflake session: {str(e)}")
            raise CustomException(f"Error closing Snowflake session: {str(e)}")



