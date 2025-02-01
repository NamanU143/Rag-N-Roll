import pandas as pd
import logging
from src.exception import CustomException

class SnowflakeDataInserter:
    """
    Class for inserting data into Snowflake from a DataFrame.
    """

    def __init__(self, session):
        self.session = session  # Snowflake session object

    def insert_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Insert a DataFrame into a Snowflake table.

        Args:
            df (pd.DataFrame): The DataFrame to insert into Snowflake.
            table_name (str): The name of the target table in Snowflake.
        """
        try:
            # Convert the Pandas DataFrame to a Snowflake DataFrame
            snowflake_df = self.session.create_dataframe(df)
            
            # Insert the DataFrame into the Snowflake table
            snowflake_df.write.mode("append").save_as_table(table_name)
            
            logging.info(f"Successfully inserted {len(df)} rows into {table_name}.")
        except Exception as e:
            logging.error(f"Error inserting DataFrame into Snowflake: {e}")
            raise CustomException(f"Error inserting DataFrame into Snowflake: {str(e)}")