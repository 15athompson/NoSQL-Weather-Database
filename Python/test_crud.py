"""
Test the database using methods that Create/Read/Update/Delete documents in the collections
to test various scenarios

Note: The database will be modified by running this file, 
      to reset, run build_database_and_import_data.py again
"""

import random
import pprint
from datetime import datetime
from pymongo import MongoClient
from helper import DualLog, DateHelper, MongoDBHandler

# Initialise the logger
logger = DualLog.create_log("test_CRUD.log")

class CRUD_tests():
    def __init__(self, mongo_handler):
        self.mongo_handler = mongo_handler
        self.collection_weather_reports = self.mongo_handler.get_collection("weather_reports")
        self.collection_weather_stations = self.mongo_handler.get_collection("weather_stations")

    # process to create a test weather report and add readings one at a time
    def create_weather_report_adding_hourly_readings(self):
        logger.info(f"\n=== Scenario to create a new weather report, add hourly readings, and compute day summary ===")
        document_id = self.insert_new_weather_report()
        self.insert_new_weather_station_reading_london_midnight(document_id)
        self.use_aggregation_pipeline_to_compute_a_day_summary(document_id)
        self.insert_new_weather_station_reading_london_1am(document_id)
        self.use_aggregation_pipeline_to_compute_a_day_summary(document_id)
        self.insert_new_weather_station_reading_london_2am(document_id)    
        self.use_aggregation_pipeline_to_compute_a_day_summary(document_id)       
        self.get_weather_report(document_id)
        DualLog.log_section_break()

    # process to change the name of an institution throughout the database
    def rename_a_weather_station_owner(self):
        logger.info(f"\n=== Rename 'Cambridge University' to 'Science Dept. Cambridge University' ===")
        logger.info(f"First get the Distinct owner names in the weather_reports collection")
        # use a fixed date for testing but with the current time rather than the current date/time that would be used in production
        modification_date = DateHelper.get_current_time_with_fixed_day(datetime(2025, 5, 1, 0, 0)) 
        self.get_distinct_owner_names("weather_reports")

        logger.info(f"Show the version number and last modified date of a weather report BEFORE update")
        projection = {"date": 1,"version": 1,"last_modified": 1,"owner": 1}
        self.get_latest_weather_report_for_owner("camUni", projection)

        self.rename_weather_reports_owner("camUni","Science Dept. Cambridge University", modification_date)

        logger.info(f"Show the version number and last modified date of the weather report AFTER update")
        self.get_latest_weather_report_for_owner("camUni", projection)

        logger.info(f"\nGet the Distinct owner names again to prove the update has applied to all documents")
        self.get_distinct_owner_names("weather_reports")
        logger.info(f"Now get the Distinct owner names in the weather_stations collection")
        self.get_distinct_owner_names("weather_stations")
        self.rename_weather_station_owner("camUni","Science Dept. Cambridge University")
        logger.info(f"\nGet the Distinct owner names again to prove the update has applied to all documents")
        self.get_distinct_owner_names("weather_stations")

        logger.info("Finally, update the institution name in the users collection")
        self.update_institution_name("camUni","Science Dept. Cambridge University")

        DualLog.log_section_break()

    # process to generate and search a weather report storage report
    def weather_report_storage_reporting(self):
        self.weather_report_storage_size_report()
        self.get_top_five_users_of_storage()
    
    # process to test weather station status changes
    def weather_station_status_updates(self):
        logger.info(f"=== Test read and update of the status field of Weather Stations ===")
        self.find_stations_with_status("Offline")
        self.update_weather_station_status("WS-NEW002", "Offline")
        self.update_weather_station_status("WS-LIV003", "Offline")
        self.find_stations_with_status("Offline")
        self.update_weather_station_status("WS-NEW002", "Online")
        self.update_weather_station_status("WS-LIV003", "Online")
        DualLog.log_section_break()

    # process to test weather extremes report generation
    def creation_of_weather_extremes_reports(self):
        logger.info(f"=== Test the creation of a Weather Extremes report for Glasgow and Southampton ===")
        self.create_weather_extremes_report("WS-GLA001")
        self.create_weather_extremes_report("WS-SOU001")
        DualLog.log_section_break()

    # process to test weather report deletion
    def weather_report_deletion(self):
        self.count_weather_reports_for_station("WS-WW1001")
        self.delete_weather_reports("WS-WW1001",datetime(2024, 2, 1),datetime(2024, 2, 4))
        self.count_weather_reports_for_station("WS-WW1001")


    # test case to insert a new weather report for london on 10th April 2025
    def insert_new_weather_report(self):
        logger.info(f"\nInserting weather report for London 10th April 2025 - for each test, using 10th April 2025 as the modification date")
        # test report for London 10th April 2025 - based on real data - set modification time to the start of that day
        dt = datetime(2025, 4, 10, 0, 0)
        doc = {
                "date": dt, "version": 1, "last_modified": dt,
                "location": {"type": "Point", "coordinates": [-0.1630249, 51.493847]},
                "station": {"station_id": "WS-LON002","name": "London"},
                "owner": {"owner_type": "Government","user_id": "metoff1","name": "Met Office"},
                "readings": []
        }

        log_title = "\nUsing weather_reports.insert_one() to add a weather report"       

        document_id = mongo_handler.insert_one(self.collection_weather_reports, doc, logger, log_title, True)
        return document_id

    # test case to insert a new reading into a weather report for london at 12am on 10th April 2025
    def insert_new_weather_station_reading_london_midnight(self, weather_report_id):
        logger.info(f"\nInserting weather station reading for London 10th April 2025 - midnight")
        dt = datetime(2025, 4, 10, 0, 0)
        reading =  {
                    "timestamp": dt, "sample_duration": 3600, "temp": 6.5, "dewpoint": 2.5,
                    "humidity": 76, "pressure": 1032.7, "precip": 0, "cloud_cover": 25, "wind_speed": 2.62,
                    "wind_direction": 43, "sunshine": 0, 
                    "soil": {
                        "0_to_7cm": {"temp": 9.6, "moisture": 0.012},
                        "7_to_28cm": {"temp": 11.7, "moisture": 0.126},
                        "28_to_100cm": {"temp": 10.3, "moisture": 0.28},
                        "100_to_255cm": {"temp": 8, "moisture": 0.291} } }        
        
        return self.insert_new_weather_station_reading(weather_report_id, reading, dt)
        
    # test case to insert a new reading into a weather report for london at 1am on 10th April 2025
    def insert_new_weather_station_reading_london_1am(self, weather_report_id):
        logger.info(f"\nInserting weather station reading for London 10th April 2025 - 1am")
        dt = datetime(2025, 4, 10, 1, 0)
        reading = {
                    "timestamp": dt, "sample_duration": 3600, "temp": 6, "dewpoint": 1.8,
                    "humidity": 75, "pressure": 1032.7, "precip": 0, "cloud_cover": 24, "wind_speed": 2.21,
                    "wind_direction": 38, "sunshine": 0,
                    "soil": {
                        "0_to_7cm": {"temp": 8.9, "moisture": 0.01},
                        "7_to_28cm": {"temp": 11.7, "moisture": 0.126},
                        "28_to_100cm": {"temp": 10.3, "moisture": 0.28},
                        "100_to_255cm": {"temp": 8, "moisture": 0.291} } }
        
        return self.insert_new_weather_station_reading(weather_report_id, reading, dt)

    # test case to insert a new reading into a weather report for london at 2am on 10th April 2025
    def insert_new_weather_station_reading_london_2am(self, weather_report_id):
        logger.info(f"\nInserting weather station reading for London 10th April 2025 - 2am")
        dt = datetime(2025, 4, 10, 2, 0)
        reading = {
                    "timestamp": dt, "sample_duration": 3600, "temp": 5.6, "dewpoint": 2,
                    "humidity": 77, "pressure": 1032.5, "precip": 1.1, "cloud_cover": 44, "wind_speed": 2.08,
                    "wind_direction": 35, "sunshine": 0,
                    "soil": {
                        "0_to_7cm": {"temp": 8.1, "moisture": 0.01},
                        "7_to_28cm": {"temp": 11.6, "moisture": 0.126},
                        "28_to_100cm": {"temp": 10.3, "moisture": 0.279},
                        "100_to_255cm": {"temp": 8, "moisture": 0.291} } }        

        return self.insert_new_weather_station_reading(weather_report_id, reading, dt)
        
    # insert a new reading into a weather report and update the version and last_modified timestamp
    def insert_new_weather_station_reading(self, weather_report_id, reading, last_modified):
        # insert a new reading into a weather report
        filter = {"_id": weather_report_id}
        # use $push to add the reading to the readings array in the document.
        # also increment the document version and update the last modified timestamp
        update = {
                "$push": {"readings": reading},
                "$inc": {"version": 1},
                "$set": {"last_modified":last_modified} }
        
        logger.info(f"\nUsing weather_reports.update_one() to add a reading to the weather report, using this filter:")
        logger.info(pprint.pformat(filter))
        logger.info(f"And this update operation:")
        logger.info(pprint.pformat(update))
        logger.info("")

        result = mongo_handler.update_one(self.collection_weather_reports, filter, update, logger)

    # Compute a day summary for a weather report from the readings on that day
    def use_aggregation_pipeline_to_compute_a_day_summary(self, weather_report_id):
        # create an aggregation pipeline to compute the day summary for a single weather report
        pipeline = [
            {"$match": {"_id": weather_report_id}},
            {"$unwind": "$readings"},
            {"$group": {
                "_id": "$_id",
                "temp_mean": {"$avg": "$readings.temp"},
                "temp_min": {"$min": "$readings.temp"},
                "temp_max": {"$max": "$readings.temp"},
                "dewpoint_mean": {"$avg": "$readings.dewpoint"},
                "humidity_mean": {"$avg": "$readings.humidity"},
                "humidity_min": {"$min": "$readings.humidity"},
                "humidity_max": {"$max": "$readings.humidity"},
                "pressure_mean": {"$avg": "$readings.pressure"},
                "pressure_min": {"$min": "$readings.pressure"},
                "pressure_max": {"$max": "$readings.pressure"},
                "precip_sum": {"$sum": "$readings.precip"},
                "cloud_cover_mean": {"$avg": "$readings.cloud_cover"},
                "wind_speed_mean": {"$avg": "$readings.wind_speed"},
                "wind_speed_min": {"$min": "$readings.wind_speed"},
                "wind_speed_max": {"$max": "$readings.wind_speed"},
                "sunshine": {"$sum": "$readings.sunshine"} } } ]

        logger.info(f"\nComputing the day_summary sub-document using the pipeline:")
        logger.info(pprint.pformat(pipeline))        
        result = self.mongo_handler.aggregate( self.collection_weather_reports, pipeline, logger )

        if result:
            # write the day_summary back into the document and increment the version
            day_summary = result[0]
            day_summary.pop("_id")  # Remove _id before updating

            filter = {"_id": weather_report_id}
            update = {
                "$inc": {"version": 1},
                "$set": {"day_summary":day_summary} }
            
            logger.info(f"\nAdd the day_summary into the weather_report using update_one() and increment the version number, using this filter:")
            logger.info(pprint.pformat(filter))
            logger.info(f"And this update operation:")
            logger.info(pprint.pformat(update))
            logger.info("")
            
            self.mongo_handler.update_one( self.collection_weather_reports, filter, update, logger)
    
    # Find a weather report matching the specified id
    def get_weather_report(self, weather_report_id):        
        search = {"_id": weather_report_id}
        report = self.collection_weather_reports.find_one(search)
        logger.info(f"\nWeather Report:")
        logger.info(pprint.pformat(report))
    
    # update the name of an owner in the weather reports collection
    # - increments the version number and the last_modified date
    def rename_weather_reports_owner(self, user_id, new_name, last_modified):
        filter = {"owner.user_id": user_id}
        update = {
                    "$set": {
                        "owner.name": new_name,
                        "last_modified":last_modified
                    },
                    "$inc": {"version": 1},
                }
        
        logger.info(f"\nUsing weather_reports.update_many() to rename the owner of the documents matching:")
        logger.info(pprint.pformat(filter))
        logger.info(f"By applying the update operation:")
        logger.info(pprint.pformat(update))
        logger.info("")

        result = mongo_handler.update_many(self.collection_weather_reports, filter, update, logger)

    # Update the name of the owner of a weather station
    def rename_weather_station_owner(self, user_id, new_name):
        filter = {"owner.user_id": user_id}
        update = {"$set": {"owner.name": new_name}}
        
        logger.info(f"\nUsing weather_stations.update_many() to rename the owner of the documents matching:")
        logger.info(pprint.pformat(filter))
        logger.info(f"By applying the update operation:")
        logger.info(pprint.pformat(update))
        logger.info("")
        
        result = mongo_handler.update_many(self.collection_weather_stations, filter, update, logger)

    # Update the name of an institution matching the user_id in the users collection
    def update_institution_name(self, user_id, new_name):
        logger.info(f"\nUsing users.update_one() to set the name of {user_id} to {new_name}")
        
        # Use update_one as there will only be one document per id
        filter = {"_id": user_id}
        update = {"$set": {"institution": new_name}}

        logger.info(pprint.pformat(filter))
        logger.info(f"By applying the update operation:")
        logger.info(pprint.pformat(update))

        collection = mongo_handler.get_collection("users")
        result = self.mongo_handler.update_one( collection, filter, update, logger)
        
        if result.modified_count > 0:
            logger.info(f"Name updated")
            logger.info("")      
            return True
        else:
            logger.info(f"Failed to update name")
            logger.info("")
            return False

    # Get distinct owner names from the collection
    def get_distinct_owner_names(self, collection_name):
        collection = mongo_handler.get_collection(collection_name)
        distinct_names = collection.distinct("owner.name")
        logger.info(f"\nUsing {collection_name}.distinct(\"owner.name\") to get distinct owner names in {collection_name}:")
        for name in distinct_names:
            logger.info(f"- {name}")

    # Get the most recent weather report for a user_id
    def get_latest_weather_report_for_owner(self, user_id, projection):
        search = {"owner.user_id": user_id}
        report = self.collection_weather_reports.find_one(search,projection,sort=[("date", -1)]) # Sort by most recent date
        logger.info(f"Latest weather report for {user_id}:")
        logger.info(pprint.pformat(report))


        
    # Build a report that details the storage size of the weather reports split by month & owner and 
    # add the report to the database
    def weather_report_storage_size_report(self):
        logger.info(f"Preparing the Weather Report Storage Size Report")

        # create an aggregation pipeline to build the report and save it into a new collection

        pipeline = [
            {"$addFields": {
                "doc_size_bytes": { "$bsonSize": "$$ROOT" },
                "year": { "$year": "$date" },
                "month": { "$month": "$date" },
                "owner_id": "$owner.user_id"} },
            {"$group": {
                "_id": {
                    "year": "$year",
                    "month": "$month",
                    "owner_id": "$owner_id"},
                "total_storage_bytes": { "$sum": "$doc_size_bytes" },
                "document_count": { "$sum": 1 } } },
            {"$out": "weather_report_storage_report" } ]        

        logger.info(f"\n=== Preparing the Weather Report Storage Size Report ===")
        logger.info(f"\nPipeline for weather_reports.aggregate():")
        logger.info(pprint.pformat(pipeline))

        self.mongo_handler.aggregate( self.collection_weather_reports, pipeline )

    # Use the weather_report_storage_report to get the top five users of storage
    def get_top_five_users_of_storage(self):
        # create an aggregation pipeline to build the report
        pipeline = [
            {"$group": {
                "_id": "$_id.owner_id",
                "total_bytes": { "$sum": "$total_storage_bytes" } } },
            {"$project": {
                "owner_id": "$_id",
                "_id": 0,
                "total_storage_MB": {
                    "$round": [
                        { "$divide": ["$total_bytes", 1024 * 1024] }, 2 ] } } },
            { "$sort": { "total_storage_MB": -1 } },
            { "$limit": 5 } ]

        logger.info(f"\n=== Preparing the Top Five Users of Storage Report ===")
        logger.info(f"Get the top five users of database storage (with regard to weather reports)")
        logger.info(f"\nPipeline for weather_report_storage_report.aggregate():")
        logger.info(pprint.pformat(pipeline))

        collection = self.mongo_handler.get_collection('weather_report_storage_report')
        result = self.mongo_handler.aggregate( collection, pipeline, logger )

        logger.info(pprint.pformat(result))
        DualLog.log_section_break()             
                
    # Update the status of a weather station
    def update_weather_station_status(self, station_id, status):
        logger.info(f"\n=== Update the weather station {station_id} status to {status} ===")
        
        # Use update_one as there will only be one document per id
        filter = {"_id": station_id}
        update = {"$set": {"status": status}}

        logger.info(pprint.pformat(filter))
        logger.info(f"By applying the update operation:")
        logger.info(pprint.pformat(update))

        result = self.mongo_handler.update_one( self.collection_weather_stations, filter, update, logger)
        
        if result.modified_count > 0:
            logger.info(f"Status updated")
            return True
        else:
            logger.info(f"Failed to update status")
            return False
    
    # Find all weather stations with the specified status
    def find_stations_with_status(self, status):

        search = {"status": status}

        results = self.collection_weather_stations.find(search)

        logger.info(f"=== Weather Stations with status of {status} ===")
        logger.info(f"\nSearch criteria for weather_stations.find():")
        logger.info(pprint.pformat(search))
        DualLog.log_results(results)
        
    # Create a report detailing extreme weather events
    def create_weather_extremes_report(self, station_id):
        # create an aggregation pipeline to build the report
        pipeline = [
            {"$match": {
                "station.station_id": station_id,
                "$or": [
                    {"day_summary.temp_min": {"$lt": -5}},  # Very cold temperatures
                    {"day_summary.temp_max": {"$gt": 28}},  # Very hot temperatures
                    {"day_summary.wind_speed_max": {"$gt": 12}},  # Strong winds
                    {"day_summary.precip_sum": {"$gt": 15}}  # Heavy precipitation
                ] } },
            {"$addFields": {
                "extreme_temp": {
                    "$cond": [
                        {"$lt": ["$day_summary.temp_min", 0]},
                        "Very Cold",
                        {"$cond": [
                            {"$gt": ["$day_summary.temp_max", 28]},
                            "Very Hot",
                            None ] } ] },
                "extreme_weather": {
                    "$cond": [
                        { "$and": [
                            { "$gt": ["$day_summary.wind_speed_max", 12] },
                            { "$gt": ["$day_summary.precip_sum", 15] }] },
                        "Storm",
                        {"$cond": [
                            { "$gt": ["$day_summary.wind_speed_max", 12] },
                            "Strong Winds",
                            {"$cond": [
                                { "$gt": ["$day_summary.precip_sum", 15] },
                                "Heavy Rain",
                                None ] } ] } ] } } },
            { "$project": {
                "_id": {
                    "$concat": [
                        {"$toString": "$station.station_id"},
                        "-",
                        {"$dateToString": {"format": "%Y%m%d", "date": "$date"}},                        
                    ]
                },
                "date": 1,
                "station_id": "$station.station_id",
                "station_name": "$station.name",
                "extreme_temp": 1,
                "extreme_weather": 1,
                "temp_min": "$day_summary.temp_min",
                "temp_max": "$day_summary.temp_max",
                "wind_speed_max": "$day_summary.wind_speed_max",
                "precip_sum": "$day_summary.precip_sum" } },
            {"$merge": {
                "into": "weather_extremes",
                "on": "_id",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            } } ]

        logger.info(f"\n=== Preparing the Weather Extremes Report for Station {station_id }===")
        logger.info(f"\nPipeline for weather_reports.aggregate():")
        logger.info(pprint.pformat(pipeline))

        self.mongo_handler.aggregate( self.collection_weather_reports, pipeline )

        # Note: As the pipeline contains a $merge command it is not possible to report on how many
        # documents were replaced/inserted, so the collection can be checked using Compass
        DualLog.log_section_break()       

    # Count weather reports for a weather station
    def count_weather_reports_for_station(self, station_id):       
        filter = {"station.station_id": station_id}

        logger.info(f"\n=== Count Weather Report for Station {station_id} ===")
        logger.info(f"\nFilter for weather_reports.count_documents():")
        logger.info(pprint.pformat(filter))
        
        self.mongo_handler.count_documents( self.collection_weather_reports, filter, logger)
        
        DualLog.log_section_break()

    # Delete weather reports from start to end date inclusive
    def delete_weather_reports(self, station_id, start, end):
        filter = {
                    "station.station_id": station_id,
                    "date": {"$gte": start, "$lte": end}}

        logger.info(f"\n=== Delete Weather Report for Station {station_id} from {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')} ===")
        logger.info(f"\nFilter for weather_reports.delete_many():")
        logger.info(pprint.pformat(filter))
        self.mongo_handler.delete_many( self.collection_weather_reports, filter, logger)
        DualLog.log_section_break()

# Main script to test data modification scenarios on the weather database
if __name__ == "__main__":
    
    # seed the random number generator to create consistent data on each run
    random.seed(99)

    # Initialise a connection to the database server
    client = MongoClient("mongodb://localhost:27017/")
    db_name='weather'

    # Initialise MongoDB handler for using the database
    mongo_handler = MongoDBHandler(client, db_name)

    # run CRUD tests
    tests = CRUD_tests(mongo_handler)
    # essay Test 11 - test weather station status modification
    tests.weather_station_status_updates() 
    # essay Test 12 - create a weather report, add a reading, compute and update day summary
    tests.create_weather_report_adding_hourly_readings() 
    # essay Test 13 - delete from the weather reports collection
    tests.weather_report_deletion() 
    # essay Test 14 - change the name of an institution 
    tests.rename_a_weather_station_owner() 
    # essay Test 15 - create a report detailing storage usage by month and user
    tests.weather_report_storage_reporting()
    # essay Test 16 - create a report in the database that details extreme weather events
    tests.creation_of_weather_extremes_reports() 
