import logging
import pandas as pd
from snowflake.snowpark import Session
import logging  # Assuming you are using your custom logger


class SnowflakeDataInserter:
    """
    Class to insert DataFrame data into a Snowflake table using the Snowflake session.
    """

    def __init__(self, session: Session):
        """
        Initialize the SnowflakeDataInserter with a Snowflake session.
        Args:
            session (Session): Snowflake session object
        """
        self.session = session
        logging.info("SnowflakeDataInserter initialized successfully.")

    def insert_df(self, dataframe: pd.DataFrame, table_name: str):
        """
        Insert the given pandas DataFrame into the specified Snowflake table.
        Args:
            dataframe (pd.DataFrame): DataFrame containing data to insert
            table_name (str): Name of the Snowflake table
        """
        try:
            logging.info(f"Inserting DataFrame into table {table_name}.")
            
            # Use write_pandas to insert DataFrame into Snowflake
            self.session.write_pandas(dataframe, table_name, auto_create_table=True)
            
            logging.info(f"Data successfully inserted into table {table_name}.")
        except Exception as e:
            logging.error(f"Error inserting data into Snowflake table {table_name}: {str(e)}")
            raise Exception(f"Error inserting data into Snowflake table {table_name}: {str(e)}")
