"""
Helper classes used by the python files to work with the database
"""

import pprint
import os
import logging
import json
from datetime import datetime, timezone

class DateHelper:
    @staticmethod
    # convert a datetime in the format 2023-01-01T00:00 to a utc time suitable for MongoDB
    def parse_csv_datetime(time_str):
        return datetime.strptime(time_str, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)

    # convert a date in the format 2023-01-01 to a utc time suitable for MongoDB
    def parse_csv_date(date_str):
        return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    
    # return the date with the time set to 23:59:59
    def get_end_of_day(dt):
        return dt.replace(hour=23, minute=59, second=59)
    
    # return the current system hh:mm:ss applied to the date parameter
    def get_current_time_with_fixed_day(dt):
        now = datetime.now()
        return dt.replace(hour=now.hour, minute=now.minute, second=now.second)

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)
    
class DualLog:
    logger = None

    #Create a logger so that logged output is sent to terminal and a file
    #  
    @staticmethod
    def create_log(filename):
        DualLog.logger = logging.getLogger('mongo_logger')
        DualLog.logger.setLevel(logging.INFO)

        # Create output directory if it doesn't exist
        output_dir = os.path.join(os.path.dirname(__file__), "output")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # File handler    
        output_file = os.path.join(output_dir, filename)
        file_handler = logging.FileHandler(output_file)
        file_handler.setLevel(logging.INFO)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Formatter
        formatter = logging.Formatter('%(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers so that logger outputs to both terminal and file
        DualLog.logger.addHandler(file_handler)
        DualLog.logger.addHandler(console_handler)
        return DualLog.logger

    @staticmethod
    def log_results(cursor, limit=10000):
        DualLog.logger.info(f"\nResults:")
        count = 0
        for doc in cursor:
            if (count < limit):
                DualLog.logger.info(pprint.pformat(doc))
            count += 1

        if count > limit:
            DualLog.logger.info(f"... (shown {limit} of {count} results)")

        if count == 0:
            DualLog.logger.info("No results found.")

        return count

    @staticmethod
    def log_section_break():
        DualLog.logger.info(f"\n=================================================================================================================\n")

# Helper class to use some of the commonly used features of MongoClient
# and write information to the Logger
class MongoDBHandler:
    def __init__(self, client, db_name):
        self.db = client[db_name]

    def get_collection(self, collection_name):
        return self.db[collection_name]
    
    def delete_collection(self, collection_name):
        # Delete the collection if it exists
        if collection_name in self.db.list_collection_names():
            self.db.drop_collection(collection_name)

    def insert_many(self, collection, documents, logger, log_title = None, log_first_document = False, log_all_documents = False, log_section_break = False):
        #Insert list of documents into MongoDB collection

        if log_title is not None:
            logger.info(log_title)

        if documents:
            # Log the first document if requested
            if log_first_document:
                logger.info("First of the documents being inserted:")
                logger.info(pprint.pformat(documents[0]))
            elif log_all_documents:
                logger.info("Documents being inserted:")
                for doc in documents:
                    logger.info(pprint.pformat(doc))

            result = collection.insert_many(documents)
            logger.info(f'\nInserted {len(result.inserted_ids)} documents.')
        else:
            logger.info('\nNo documents to insert.')
        
        if (log_section_break):
            DualLog.log_section_break()

    def insert_one(self, collection, document, logger, log_title = None, log_document = False, log_section_break = False):
        #Insert document into MongoDB collection

        if log_title is not None:
            logger.info(log_title)
    
        if document:
            # Log the document if requested
            if log_document:
                logger.info("Document being inserted:")
                logger.info(pprint.pformat(document))

            result = collection.insert_one(document)
            logger.info(f'Inserted {result.inserted_id}')

            if (log_section_break):
                DualLog.log_section_break()

            return result.inserted_id
        else:
            logger.info('No document to insert.')

            if (log_section_break):
                DualLog.log_section_break()

            return None
        

        
    def update_one(self, collection, filter, update, logger):
        #update a document in MongoDB
        update_result = collection.update_one(filter, update)
        logger.info(f'Modified {update_result.modified_count} document')
        return update_result
    
    def update_many(self, collection, filter, update, logger):
        #update documents in MongoDB
        update_result = collection.update_many(filter, update)
        logger.info(f'Modified {update_result.modified_count} documents')
        return update_result

    def aggregate(self, collection, pipeline, logger = None):
        #run an aggregation pipeline
        result = list(collection.aggregate(pipeline))
        if logger:
            if not result:
                logger.info("No matching document.")
            else:
                logger.info(f"Aggregation found {len(result)} documents.")
        return result
    
    def delete_one(self, collection, filter, logger):
        #delete a document in MongoDB
        delete_result = collection.delete_one(filter)
        logger.info(f'Deleted {delete_result.deleted_count} document')
        return delete_result
    
    def delete_many(self, collection, filter, logger):
        #delete documents in MongoDB
        delete_result = collection.delete_many(filter)
        logger.info(f'Deleted {delete_result.deleted_count} documents')
        return delete_result
    
    def count_documents(self, collection, filter, logger):
        #count documents matching filter in MongoDB
        count = collection.count_documents(filter)
        logger.info(f'{count} documents found')
        return count
