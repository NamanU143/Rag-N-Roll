import os
from src.logger import logging
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

    # def __init__(self,snowflake_username,snowflake_account,snowflake_password,snowflake_role,snowflake_warehouse,snowflake_database,snowflake_schema):
    def __init__(self):
        try:
            # setup_logger()

            # Initialize Snowflake session
            logging.info(">> Initializing Snowflake Session Parameters ")
            snowflake_username = st.secrets["snowflake"]["username"]
            snowflake_password = st.secrets["snowflake"]["password"]
            snowflake_account = st.secrets["snowflake"]["account"]
            snowflake_role = st.secrets["snowflake"]["role"]
            snowflake_warehouse = st.secrets["snowflake"]["warehouse"]
            snowflake_database = st.secrets["snowflake"]["database"]
            snowflake_schema = st.secrets["snowflake"]["schema"]

            self.connection_parameters = {
                "account": snowflake_account ,
                "user": snowflake_username,
                "password": snowflake_password,
                "role": snowflake_role,
                "warehouse": snowflake_warehouse,
                "database": snowflake_database,
                "schema": snowflake_schema
            }
            logging.info(">> Snowflake Session Parameters Initialized")

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

            logging.info(">> Successfully loaded connection parameters from secrets file.")
            
            self.session = Session.builder.configs(self.connection_parameters).create()
            self.root = Root(self.session)
            logging.info(">> Snowflake session created successfully.")
        
        except Exception as e:
            logging.error(f"Error initializing Snowflake session: {str(e)}")
            raise CustomException(f"Error initializing Snowflake session {str(e)}")

    def get_session(self):
        """
        Returns the Snowflake session object.
        """
        try:
            logging.info(f">> Returning the Snowflake session object :\n\t {self.session}")
            logging.info(f">> Returning the Snowflake session root object :\n\t {self.root}")
            return self.session
        except Exception as e:
            logging.error(f"Error retrieving Snowflake session: {str(e)}")
            raise CustomException(f"Error retrieving Snowflake session: {str(e)}")
    def get_root(self):
        """
        Returns the Snowflake session object.
        """
        try:
            logging.info(f"Returning the Snowflake session root object :\n\t {self.root}")
            return self.root
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

