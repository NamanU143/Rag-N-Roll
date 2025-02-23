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
            logging.info("Initializing SnowflakeDataTypeMapper...")
            self.dtype_mapping = {
                "int64": "NUMBER",
                "float64": "NUMBER",
                "object": "STRING",
                "datetime64[ns]": "DATE",
                "bool": "BOOLEAN",
                "category": "STRING"
            }
            logging.info("SnowflakeDataTypeMapper initialized successfully with predefined dtype mappings.")
        except Exception as e:
            logging.error(f"Error initializing SnowflakeDataTypeMapper: {str(e)}", exc_info=True)
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
            if not isinstance(dataframe, pd.DataFrame):
                logging.error("Input provided is not a pandas DataFrame.")
                raise ValueError("Expected a pandas DataFrame.")

            logging.info(f"Starting data type mapping for {len(dataframe.columns)} columns...")

            for col in dataframe.columns:
                dtype = str(dataframe[col].dtype)

                # Log the detected pandas data type
                logging.debug(f"Column '{col}' detected as pandas dtype: {dtype}")

                # Map pandas dtype to Snowflake data types
                mapped_type = self.dtype_mapping.get(dtype, "STRING")

                # Log the mapped Snowflake data type
                logging.debug(f"Column '{col}' mapped to Snowflake dtype: {mapped_type}")

                column_data_types[col] = mapped_type
            logging.info(f"Column datatypes -- {column_data_types}")
            logging.info("Data types mapping completed successfully.")
            return column_data_types

        except Exception as e:
            logging.error(f"Error mapping column data types: {str(e)}", exc_info=True)
            raise CustomException(f"Error mapping column data types: {str(e)}")
