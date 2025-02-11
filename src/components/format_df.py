import pandas as pd
import logging
from src.exception import CustomException

class SnowflakeDataTypeMapper:
    """
    Class to map pandas DataFrame column data types to Snowflake-compatible data types.
    """

    def __init__(self):
        """
        Initializes the SnowflakeDataTypeMapper class.
        """
        try:
            logging.info("SnowflakeDataTypeMapper initialized successfully.")
        except Exception as e:
            logging.error(f"Error initializing SnowflakeDataTypeMapper: {str(e)}")
            raise CustomException(f"Error initializing SnowflakeDataTypeMapper: {str(e)}")

    def get_column_data_types(self, dataframe: pd.DataFrame):
        """
        Convert DataFrame column data types to Snowflake-compatible data types.

        Args:
            dataframe (pd.DataFrame): The pandas DataFrame to map column data types.

        Returns:
            dict: A dictionary where keys are column names and values are Snowflake data types.
        """
        column_data_types = {}

        try:
            for col in dataframe.columns:
                dtype = dataframe[col].dtype
                # Map pandas dtype to Snowflake data types
                if dtype == 'int64':
                    column_data_types[col] = 'NUMBER'
                elif dtype == 'float64':
                    column_data_types[col] = 'NUMBER'
                elif dtype == 'object':  # For string or object types in pandas
                    column_data_types[col] = 'STRING'
                elif dtype == 'datetime64[ns]':
                    column_data_types[col] = 'DATE'
                else:
                    column_data_types[col] = 'STRING'  # Default to STRING for unknown types

            logging.info("Data types mapping completed successfully.")
            return column_data_types

        except Exception as e:
            logging.error(f"Error mapping column data types: {str(e)}")
            raise CustomException(f"Error mapping column data types: {str(e)}")
