from src.exception import CustomException
import logging
from src.logger import setup_logger

setup_logger()

class PreprocessNewsdf:
    def __init__(self, newsdf):
        """ Initializing dataframe to class object """
        try:
            self.newsdf = newsdf
            logging.info("Initialized news dataframe for preprocessing")
        except Exception as e:
            logging.error(f"Initializing news dataframe failed: {e}")
            raise CustomException(e)

    def process_newsdf(self):
        """This function preprocesses the news dataframe by:
        1. Removing duplicate entries based on the 'description' and 'title' columns.
        2. Converting the 'date' column into a format suitable for insertion into Snowflake.
        3. Renaming all column names to uppercase.
        """
        try:
            logging.info("Starting preprocessing of news dataframe.")

            # Remove duplicate descriptions
            before_dedup = len(self.newsdf)
            self.newsdf.drop_duplicates(subset=['description', 'title'], inplace=True)
            after_dedup = len(self.newsdf)
            logging.info(f"Removed {before_dedup - after_dedup} duplicate rows based on 'description' and 'title'.")

            # Convert 'date' column to Snowflake-compatible format (YYYY-MM-DD HH:MI:SS)
            if 'date' in self.newsdf.columns:
                self.newsdf['date'] = self.newsdf['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                logging.info("Converted 'date' column to Snowflake-compatible format.")
            else:
                logging.warning("'date' column not found in dataframe.")

            # Convert column names to uppercase
            self.newsdf.columns = [col.upper() for col in self.newsdf.columns]
            logging.info("Converted all column names to uppercase.")

            logging.info("Preprocessing completed successfully.")
            return self.newsdf

        except Exception as e:
            logging.error(f"Error during preprocessing: {e}", exc_info=True)
            raise CustomException(e)
