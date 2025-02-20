# import streamlit as st
import pandas as pd
from src.logger import setup_logger
import logging
from src.exception import CustomException
from src.configuration.snowflake import SnowflakeConnector
from src.components.news_extraction import NewsExtractor
from src.components.preprocess_newsdf import PreprocessNewsdf
from src.components.database_manager import SnowflakeDatabaseManager
from src.constants.snowflakedatacreds import DATABASENAME,SCHEMA_NAME
from src.components.data_mapper import SnowflakeDataTypeMapper


# Setting up custom logger
setup_logger()

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

    def __call_preprocess_newsdf(self,newsdf : pd.DataFrame):
        try:
            logging.info("<< Calling Preprocess News Dataframe Component")
            preprocess = PreprocessNewsdf(newsdf=newsdf)
            preprocessed_newsdf = preprocess.process_newsdf()
            return preprocessed_newsdf
        except Exception as e:
            logging.error(f"Error in Preprocess News Dataframe {CustomException(e)}") 
            raise CustomException(e)

    def __call_snowflake_database_manager(self,session,df:pd.DataFrame):
        try:
            logging.info("<< Calling Database Manager Component")
            snowflakedbmanager = SnowflakeDatabaseManager(session=session)
            snowflakedbmanager.create_database()
            snowflakedbmanager.create_schema()
            snowflakedbmanager.create_table(df=df)
        except Exception as e :
            logging.error(f"Error in Snowflake Database Manager {CustomException(e)}")
            raise CustomException(e)
        

        

    # def __call_createCortexSearchService()
    # def __call_fieldExtractor()
    # def __call_summarization_and_sentiment()
            



    def initiate_pipeline(self,query):
        try:
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

            logging.info("<<< Initiating Database Manager Component ")
            self.__call_snowflake_database_manager(session=session,df=processed_newsdf)
            logging.info(f"Completed Database Manager Component >>> ")
            ###

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