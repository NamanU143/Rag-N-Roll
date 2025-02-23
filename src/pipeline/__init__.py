# import streamlit as st
import pandas as pd
from src.logger import logging
import logging
from src.exception import CustomException
from src.configuration.snowflake import SnowflakeConnector
from src.components.news_extraction import NewsExtractor
from src.components.preprocess_newsdf import PreprocessNewsdf
from src.components.database_manager import SnowflakeDatabaseManager
from src.constants.snowflakedatacreds import DATABASENAME,SCHEMA_NAME,TABLE_NAME
from src.components.create_cortex_search_service import CortexSearchServiceManager
from src.components.insert_data_into_db import SnowflakeDataInserter
from src.components.format_df import SnowflakeDataTypeMapper

# Setting up custom logger
# setup_logger()

class Pipeline:
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise CustomException(e)
        
    def __call_snowflakeConnector(self):
        try:
            logging.info("<< Calling Snowflake Connector")
            snowflakeconnector = SnowflakeConnector()
            session = snowflakeconnector.get_session()
            root = snowflakeconnector.get_root()
            return [session,root]

        except Exception as e :
            logging.error(f"Error in Snowflake Connector  {CustomException(e)}")
            raise CustomException(e)
        
    def __call_update_session(self):
        try:
            logging.info("<< Calling Update Session")
            updatesessioninstance = SnowflakeConnector()
            session = updatesessioninstance.update_session(database_name=DATABASENAME)
            return session
        except Exception as e:
            logging.error(f"Error in Update Session {CustomException(e)}")
            raise CustomException(e)
        
    def __call_newsExtractor(self,query:str):
        try:
            logging.info("<< Calling News Extractor ")
            newsextractor = NewsExtractor()
            newsdf = newsextractor.process_news(query=query)
            return newsdf
        except Exception as e :
            logging.error(f"Error in News Extractor  {CustomException(e)}")
            raise CustomException(e)

    def __call_preprocess_newsdf(self,newsdf):
        try:
            logging.info("<< Calling Preprocess News Dataframe Component")
            preprocess = PreprocessNewsdf(newsdf=newsdf)
            preprocessed_newsdf = preprocess.process_newsdf()
            return preprocessed_newsdf
        except Exception as e:
            logging.error(f"Error in Preprocess News Dataframe {CustomException(e)}") 
            raise CustomException(e)
    
    def __call_snowflake_database_manager(self,session,df):
        try:

            # This class is responsible for creating the database, schema, and table in snowflake .
            from src.components.database_manager import SnowflakeDatabaseManager
            from src.components.format_df import SnowflakeDataTypeMapper


            # -----------------------------------------------------------
            # Creating an instance of SnowflakeDatabaseManager
            db_manager = SnowflakeDatabaseManager(session)

            # Step 1: Create database
            db_name = "MAIN_RAG_DB"
            db_manager.create_database(db_name)

            # -----------------------------------------------------------
            # Step 2: Create schema
            schema_name = "MAIN_RAG_SCHEMA"
            db_manager.create_schema(db_name, schema_name)
            # -----------------------------------------------------------


            # Mapping dataframe columns to snowflake data types

            # Initialize the class
            data_type_mapper = SnowflakeDataTypeMapper()

            # Map the DataFrame columns to Snowflake-compatible dafta types
            columns = data_type_mapper.get_column_data_types(dataframe=df)

            # Print or use the resulting column data types
            print(columns)
            # -----------------------------------------------------------

            # Step 3: Create table
            table_name = "MY_NEW_TABLE"
            db_manager.create_table(db_name, schema_name, table_name, columns)
        except Exception as e :
            logging.error(f"Error in Snowflake Database Manager {CustomException(e)}")
            raise CustomException(e)

    # def __call_snowflake_database_manager(self,session,df):
    #     try:
    #         logging.info("<< Calling Database Manager Component")
    #         snowflakedbmanager = SnowflakeDatabaseManager(session=session)
    #         snowflakedbmanager.create_database(database_name=DATABASENAME)
    #         snowflakedbmanager.create_schema(database_name=DATABASENAME,schema_name=SCHEMA_NAME)
            
    #         logging.info("<<< Initiating Database Manager Component ")
    #         formatdf = SnowflakeDataTypeMapper()
    #         columns = formatdf.get_column_data_types(dataframe=df)
    #         logging.info(f"Completed Database Manager Component >>> ")
    #         ###

    #         snowflakedbmanager.create_table(database_name=DATABASENAME,schema_name=SCHEMA_NAME,table_name=TABLE_NAME,columns=columns)
    #     except Exception as e :
    #         logging.error(f"Error in Snowflake Database Manager {CustomException(e)}")
    #         raise CustomException(e)
        
    def __call_create_cortex_search_service(self,session,root):
        try:
            logging.info("<< Calling Create Cortex Search Service Component")
            cortexsearchserviceinstance = CortexSearchServiceManager(session=session,root=root)
            cortexsearchserviceinstance.create_cortex_search_service()
        except Exception as e :
            logging.error(f"Error in Cortex Search Service Component")
            raise CustomException(e)
        
    # def call_format_df(self,df):
    #     try:
    #         logging.info("<< Calling Format DF component")
    #         formatdf = SnowflakeDataTypeMapper()
    #         column_data_types = formatdf.get_column_data_types(dataframe=df)
    #         return column_data_types 
        
    #     except Exception as e :
    #         logging.error(f"Error in format df component")

        
    # def __call_data_ingestion(self,session,df):
    #     try:
    #         logging.info("<< Calling Data Ingestion Component")
    #         datainserter = SnowflakeDataInserter(session=session)
    #         datainserter.insert_df(dataframe=df)
    #     except Exception as e :
    #         logging.error(f"Error in Data Ingestion Component")
    #         raise CustomException(e)
        
    def __call_insertinto_db(self,session,df):
        try:
            logging.info("<< Calling Insert into db Component")
            datainsertintodb = SnowflakeDataInserter(session=session)
            datainsertintodb.insert_dataframe(df=df)
        except Exception as e:
            logging.error("Error in Insert Into DB component")
            raise CustomException(e)

    # def __call_fieldExtractor()
    # def __call_summarization_and_sentiment()
            



    def initiate_pipeline(self,query):
        try:
            logging.info("Starting Pipeline !!!")

            logging.info("<<< Initiating Snowflake Connector Component ")
            connection = self.__call_snowflakeConnector()
            session = connection[0]
            root = connection[1]
            logging.info(f"===> Session : {session}")
            logging.info(f"===> Root : {root}")
            logging.info("Completed Snowflake Connector Component >>>")
            ###

            logging.info("<<< Initiating News Extraction Component")
            newsdf = self.__call_newsExtractor(query=query)         # make news extractor component clearer
            logging.info(f"NewsDataframe : \n {newsdf}")
            logging.info("Completed News Extraction Component >>>")
            ###

            logging.info("<<< Initiating Preprocess News Dataframe Component")
            processed_newsdf = self.__call_preprocess_newsdf(newsdf)
            logging.info(f"Preprocessed newsdf {processed_newsdf.head()}")
            logging.info("Completed Preprocess News Dataframe Component >>>")
            ###

            print(processed_newsdf)


            logging.info("<<< Initiating Database Manager Component ")
            self.__call_snowflake_database_manager(session=session,df=processed_newsdf)
            logging.info(f"Completed Database Manager Component >>> ")
            ###


            print(processed_newsdf)
            logging.info("<<< Initiating Updating Session")
            session = self.__call_update_session()
            logging.info(f"Updated the Session >>>")
            # ###

            
            # logging.info("Preprocessing Step !!!")
            # processed_newsdf["id"] = processed_newsdf.index
            # processed_newsdf.insert(0, "id", processed_newsdf.pop("id"))
            # processed_newsdf.columns = processed_newsdf.columns.str.upper()
            # logging.info("Preprocessing Step !!!")

            logging.info("<<< Initiating Insert into DB component ")
            self.__call_insertinto_db(session=session,df=processed_newsdf)
            logging.info(" Completed Insert Into DB component >>>")
            ###

            # logging.info("<<< Initiating Data Ingestion Component")
            # self.__call_data_ingestion(session=session,df=processed_newsdf)
            # logging.info(f"Completed Data Ingestion Component >>>")
            # ###

            # logging.info("<<< Initiating Create Cortex Search Service Component")
            # self.__call_create_cortex_search_service(session=session,root=root)
            # logging.info(f"Completed Create Cortex Search Service Component >>>")
            # ###

            logging.info("Completed Pipeline !!!")
            return processed_newsdf
        except Exception as e:
            logging.error(f"Error in Pipeline {CustomException(e)}")
            raise CustomException(e)

###################################################################################################################################

# learn django for better performance and use cases .
# can create seperate pipelines for news extraction and article processing 
# There is a chance to use the data as embeddings instead of passing to cortex search service 
# (even cortex search internally uses vector search or embedding but lets create a component for vector search from scratch )
# Also I need to explore mongodb use cases for this project 
# I want to explore FAISS for data retrival 
# Lets use other opensource llm models .
# After above steps :
    # use the pretrained state of the art models and fine tune them or create a encoder decoder model from scratch for this usecase.
###################################################################################################################################

# 1. building api for this usecase and deploying on any opensource platform 
# 2. creating a frontend for this api use case 
# 3. deploying this static website on netlify or any other opensource platform.

###################################################################################################################################