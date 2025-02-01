import logging
from snowflake.snowpark import Session

class CortexSearchServiceManager:
    """
    Class to manage Cortex Search services in Snowflake.
    """

    def __init__(self, session: Session):
        """
        Initialize the CortexSearchServiceManager with a Snowflake session.

        Args:
            session (Session): Snowflake session object
        """
        self.session = session
        logging.info("CortexSearchServiceManager initialized successfully.")

    def create_cortex_search_service(self, service_name: str, table_name: str, text_column: str, attributes: list, warehouse: str, embedding_model: str, target_lag: str = '1 day'):
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
            raise