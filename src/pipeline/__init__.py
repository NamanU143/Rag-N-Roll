# import streamlit as st
import pandas as pd
from src.logger import setup_logger
import logging
from src.exception import CustomException
from src.configuration.snowflake import SnowflakeConnector
from src.components.news_extraction import NewsExtractor
from src.components.preprocess_newsdf import PreprocessNewsdf

# Setting up custom logger
setup_logger()

class Pipeline:

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
        
    def __call_newsExtractor(self,query):
        try:
            logging.info("<< Calling News Extractor ")
            newsextractor = NewsExtractor()
            newsdf = newsextractor.process_news(query=query)
            return newsdf
        except Exception as e :
            logging.error(f"Error in News Extractor  {CustomException(e)}")

    def __call_preprocess_newsdf(self,newsdf):
        try:
            logging.info("<< Calling Preprocess News Dataframe Component")
            preprocess = PreprocessNewsdf(newsdf=newsdf)
            preprocessed_newsdf = preprocess.process_newsdf()
            return preprocessed_newsdf
        except Exception as e:
            logging.error(f"Error in Preprocess News Dataframe {CustomException(e)}") 

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

            return processed_newsdf
        except Exception as e:
            logging.error(f"Error in Pipeline {CustomException(e)}")
            raise CustomException(e)