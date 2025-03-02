# import streamlit as st
import pandas as pd
from src.logger import logging
from snowflake.core import Root
import logging
from src.exception import CustomException
from configuration.snowflakeconfig import SnowflakeConnector
from src.components.news_extraction import NewsExtractor
from src.components.preprocess_newsdf import PreprocessNewsdf
from src.constants.snowflakedatacreds import DATABASENAME,SCHEMA_NAME,TABLE_NAME
from src.components.create_cortex_search_service import CortexSearchServiceManager
from src.components.insert_dataframe_to_db import SnowflakeInserter
from src.components.database_manager import SnowflakeDatabaseManager
from src.components.cortex_response_extractor import FieldExtractor

import pandas as pd

pd.set_option("display.max_rows", None)  # Show all rows
pd.set_option("display.max_columns", None)  # Show all columns


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

    def __call_database_manager(self,session):
        try:
            logging.info("<< Calling Create Cortex Search Service Component")
            dbmanager = SnowflakeDatabaseManager(session=session)
            dbmanager.create_database(database_name="MAIN_RAG_DB")
            dbmanager.create_schema(database_name="MAIN_RAG_DB",schema_name="MAIN_RAG_SCHEMA")
        except Exception as e :
            logging.error(f"Error in Cortex Search Service Component")
            raise CustomException(e)
        
    def __call_datainserter(self,df):
        try:
            logging.info("<< Calling Create Cortex Search Service Component")
            dbinserter = SnowflakeInserter()
            dbinserter.insert_dataframe(df=df)
        except Exception as e :
            logging.error(f"Error in Cortex Search Service Component")
            raise CustomException(e)

    def __call_create_cortex_search_service(self,session,root):
        try:
            logging.info("<< Calling Create Cortex Search Service Component")
            cortexsearchserviceinstance = CortexSearchServiceManager(session=session,root=root)
            cortexsearchserviceinstance.create_cortex_search_service()
        except Exception as e :
            logging.error(f"Error in Cortex Search Service Component")
            raise CustomException(e)
        
    def __call_cortex_response_extractor(self,result):
        try:
            logging.info("<< Calling cortex Query Extractor")
            cortexresp = FieldExtractor()
            response = cortexresp.extract_fields(result=result)
            return response
        except Exception as e:
            logging.error(f"Error in Cortex Response Extractor Component")
            raise CustomException(e)

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
            # logging.info(f"Preprocessed newsdf:\n{processed_newsdf.to_string()}")
            logging.info("Completed Preprocess News Dataframe Component >>>")
            ###

            print(processed_newsdf)

            logging.info("<<< Initiating Database Manager Component")
            self.__call_database_manager(session=session)
            logging.info("Completed Database Manager Component >>>")

            logging.info("<<< Initiating Insert into db Component")
            ### Inserting a dataframe into snowflake table 
            self.__call_datainserter(df=processed_newsdf)
            logging.info("Completed Insert into db Component >>>")

            logging.info("<<< Initiating Create Cortex Search Service Component")
            self.__call_create_cortex_search_service(session=session,root=root)
            logging.info("Completed Creating Cortex Search Service Component >>>")
            
            # Call search service
            logging.info("<<< Calling Search Service Into Pipeline")
            root = Root(session)
            news_search_service = root.databases["MAIN_RAG_DB"].schemas["MAIN_RAG_SCHEMA"].cortex_search_services["news_search"]
            resp = news_search_service.search(query=query, columns=["DATE", "SOURCE", "CONTENT", "AUTHOR", "URL", "DESCRIPTION"], limit=1)
            logging.info("<<< Completed Search Service Into Pipeline")


            # filter the embedded data from the cortex search service
            logging.info("<<< Calling Cortex Response Extractor Component")
            cortexresp = self.__call_cortex_response_extractor(result=resp)
            logging.info("Completed Cortex Response Extractor Component >>>")

            
            # Here as we are streaming the output the moment we get the data from our agents we have to directly call the ai agents 
            # in the ui or streamlit app else the stream might not work

            # passing the response to trade assistllm component

            # Implement Conversation History


            # when pipeline ends truncate the results for a single user and store the previous results in the conversation history
            logging.info("Completed Pipeline !!!")
            return cortexresp
        except Exception as e:
            logging.error(f"Error in Pipeline {CustomException(e)}")
            raise CustomException(e)

###################################################################################################################################

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