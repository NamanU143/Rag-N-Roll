from src.logger import logging
from snowflake.snowpark import Session
from src.constants.snowflakedatacreds import DATABASENAME,SCHEMA_NAME,TABLE_NAME
from src.exception import CustomException

class CortexSearchServiceManager:
    """
    Class to manage Cortex Search services in Snowflake.
    """

    def __init__(self, session: Session,root):
        """
        Initialize the CortexSearchServiceManager with a Snowflake session.

        Args:
            session (Session): Snowflake session object
        """
        self.session = session
        self.root = root
        logging.info("CortexSearchServiceManager initialized successfully.")

    def create_cortex_search_service(self):
        """
        Create or replace a Cortex Search service.

        Args:
            service_name (str): Name of the Cortex Search service.
            table_name (str): Name of the source table.
            text_column (str): Column containing the text to be indexed.
            attributes (list): List of columns to include as attributes.
            warehouse (str): Snowflake warehouse to use.
            embedding_model (str): Embedding model for the service.
            target_lag (str): Data lag target for indexing (default is '1 day').
        """
        try:
            # Construct the SQL for creating the Cortex Search service
            table_name = "MY_NEW_TABLE"
            service_name = "news_search"
            warehouse = "COMPUTE_WH"
            embedding_model = "snowflake-arctic-embed-l-v2.0"
            target_lag = "1 day"
            attributes = ["ID","SOURCE", "AUTHOR", "DATE", "TITLE", "URL", "DESCRIPTION"]
            text_column = "CONTENT"

            attributes_str = ", ".join(attributes)  # Convert attributes list to a comma-separated string

            create_search_service_sql = f"""
            CREATE OR REPLACE CORTEX SEARCH SERVICE {service_name}
            ON {text_column}
            ATTRIBUTES {attributes_str}
            WAREHOUSE = {warehouse}
            TARGET_LAG = '{target_lag}'
            EMBEDDING_MODEL = '{embedding_model}'
            AS (
                SELECT
                    {text_column}, {attributes_str}
                FROM {table_name}
            );
            """

            # Execute the SQL statement
            self.session.sql(create_search_service_sql).collect()
            logging.info(f"Cortex Search service '{service_name}' created successfully.")
        except Exception as e:
            logging.error(f"Error creating Cortex Search service '{service_name}': {e}")
            raise CustomException(e)
        except IndexError as e:
            logging.error(f"Index out of range error msg :{e}")
    
    # def search_financial_news(self,stock_query):
    #     """
    #     Searches for financial news related to a given stock query using Cortex Search.

    #     Args:
    #         root: The root database object containing the search service.
    #         stock_query (str): The stock ticker or query to search for.
    #         limit (int, optional): The number of results to fetch. Defaults to 1.

    #     Returns:
    #         dict: The search response containing relevant news articles.
    #     """
    #     try :
    #         limit = 1
    #         news_search_service = self.root.databases["MAIN_RAG_DB"].schemas["MAIN_RAG_SCHEMA"].cortex_search_services["news_search"]
    #         return news_search_service.search(
    #             query=stock_query,
    #             columns=["DATE", "SOURCE", "CONTENT", "AUTHOR", "URL", "DESCRIPTION"],
    #             limit=limit
    #         )

    #     except Exception as e :
    #         logging.error(f"Error in Searching Financial News {CustomException(e)}")
    #         raise CustomException(e)
