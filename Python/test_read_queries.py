"""
Test the database using Read Queries

Tests the implementation of the MongoDB Weather database by implementing potential enquiries
Note: The database will NOT be modified be running these queries
"""

import pprint
from pymongo import MongoClient
from datetime import datetime, timedelta
from helper import DualLog
from encrypt import EncryptionHelper

# Initialise the logger
logger = DualLog.create_log("test_read_queries.log")

def validate_user(db, entered_user_id, entered_password):
    """
    Query: Get a user's details to validate login

    Args:
        db (Database): The weather database
        entered_user_id (string): The user id to find
        entered_password (string): The password to validate
    """
    valid = False # default to invalid

    # Find user
    search = {
                "_id": entered_user_id
            }

    user = db.users.find_one(search)
    logger.info(f"\n=== Validate User {entered_user_id} password: {entered_password} ===")
    logger.info(f"\nSearch criteria for users.find_one():")
    logger.info(pprint.pformat(search))
    logger.info(f"\nResult:")

    if (user is None):
        logger.info(f"\nUser not found")
    else:
        logger.info(pprint.pformat(user))
        if (EncryptionHelper.verify_password(entered_password, user.get("password",""))):
            logger.info(f"\nPassword valid")
            valid = True

            # if Private / Admin user, decrypt and display the name and email address
            if (user.get("user_type","") == "Private" or user.get("user_type","") == "Admin"):
                logger.info(f"\nDecrypted name: {EncryptionHelper.decrypt( user.get("name"))}")
                logger.info(f"Decrypted email: {EncryptionHelper.decrypt( user.get("email"))}")
        else:
            logger.info(f"\nInvalid password")            

    DualLog.log_section_break()
    return valid


def find_stations_near_location(db, longitude, latitude, max_distance_miles=50):
    """
    Query: Find all weather stations within a radius around a geographic location

    Args:
        db (Database): The weather database
        longitude (float): Longitude coordinate
        latitude (float): Latitude coordinate
        max_distance_miles (int): Maximum distance in miles
    """

    # Earth's radius in miles
    earth_radius_miles = 3963.2

    # Convert distance to radians
    radius_radians = max_distance_miles / earth_radius_miles
    
    search = {
                "location": {
                    "$geoWithin": {
                        "$centerSphere": [[longitude, latitude], radius_radians]
                    }
                }
            }

    results = db.weather_stations.find(search)

    logger.info(f"=== Weather Stations within {max_distance_miles} miles of [{longitude}, {latitude}] ===")
    logger.info(f"\nSearch criteria for weather_stations.find():")
    logger.info(pprint.pformat(search))
    DualLog.log_results(results)
    DualLog.log_section_break()


def get_weather_report(db, station_id, search_date):
    """
    Query: Get a weather report for a specific weather station and date

    Args:
        db (Database): The weather database
        station_id (string): The ID of the weather station.
        search_date (datetime): The date of interest
    """
    next_day = search_date + timedelta(days=1)

    # Find weather report for the search date
    search = {
                "station.station_id": station_id,
                "date": {"$gte": search_date, "$lt": next_day}
            }

    report = db.weather_reports.find_one(search)
    #print(f"\n=== Weather Report for Station {station_id} on {search_date.strftime('%Y-%m-%d')} ===")
    #pp.pprint(report)
    logger.info(f"\n=== Weather Report for Station {station_id} on {search_date.strftime('%Y-%m-%d')} ===")
    logger.info(f"\nSearch criteria for find_one():")
    logger.info(pprint.pformat(search))
    logger.info(f"\nResult:")
    logger.info(pprint.pformat(report))
    DualLog.log_section_break()

    return report



def calculate_average_temperature_time_range(db, station_id, start_hour=10, end_hour=15):
    """
    Query: Calculate the average temperature between start and end times for a specific weather station

    Args:
        db (Database): The weather database
        station_id (str): The ID of the weather station
        start_hour (int): Start hour (24-hour format)
        end_hour (int): End hour (24-hour format)
    """
    # Find all readings for the station within the time range
    pipeline = [
        {"$match": {"station.station_id": station_id}},
        {"$unwind": "$readings"},
        {"$project": {
            "station_name": "$station.name",
            "date": "$date",
            "timestamp": "$readings.timestamp",
            "temperature": "$readings.temp",
            "hour": {"$hour": "$readings.timestamp"}
        }},
        {"$match": {
            "hour": {"$gte": start_hour, "$lt": end_hour}
        }},
        {"$group": {
            "_id": {"date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date"}}
            },
            "station_name": {"$first": "$station_name"},
            "avg_temperature": {"$avg": "$temperature"},
            "reading_count": {"$sum": 1}
        }},
        {"$sort": {"_id.date": -1}}
    ]

    results = db.weather_reports.aggregate(pipeline)

    logger.info(f"=== Average Temperature between {start_hour}:00 and {end_hour}:00 for Station {station_id} ===")
    logger.info(f"\nPipeline for weather_reports.aggregate():")
    logger.info(pprint.pformat(pipeline))
    count = DualLog.log_results(results)
    logger.info(f"Total days with data: {count}")
    DualLog.log_section_break()


def calculate_average_temperature_area(db, longitude, latitude, radius_miles, start_hour, end_hour, start_date, end_date):
    """
    Query: Calculate the average temperature over a range of hours for data collected 
        in a date range, for each weather station within a certain distance of a geographic location 

    Args:
        db (Database): The weather database
        longitude (float): Longitude coordinate
        latitude (float): Latitude coordinate
        radius_miles (int): Radius in miles
        start_hour (int): Start hour (24-hour format)
        end_hour (int): End hour (24-hour format)
        start_date (datetime): Start of the date range for the data match
        end_date (datetime): End of the date range for the data match
    """
    # Convert miles to meters
    radius_meters = radius_miles * 1609.34

    # Now calculate average temperature for each station
    pipeline = [
        {
            "$geoNear": {
                "near": {
                    "type": "Point",
                    "coordinates": [longitude, latitude]
                },
                "distanceField": "distance",
                "maxDistance": radius_meters,
                "spherical": True,
                "includeLocs": "location"
            }
        },
        {"$unwind": "$readings"},
        {
            "$project": {
                "station_id": "$station.station_id",
                "station_name": "$station.name",
                "date": "$date",
                "timestamp": "$readings.timestamp",
                "temperature": "$readings.temp",
                "hour": {"$hour": "$readings.timestamp"},
                "distance": "$distance"
            }
        },
        {"$match": {
            "hour": {"$gte": start_hour, "$lt": end_hour},
            "timestamp": {
                "$gte": start_date,
                "$lt": end_date
            }
        }},
        {"$group": {
            "_id": {
                "station_id": "$station_id",
                "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date"}}
            },
            "station_name": {"$first": "$station_name"},
            "distance": { "$first": "$distance" },
            "avg_temperature": {"$avg": "$temperature"},
            "reading_count": {"$sum": 1}
        }},
        {"$group": {
            "_id": "$_id.station_id",
            "station_name": {"$first": "$station_name"},
            "distance": {"$first": "$distance"},
            "overall_avg_temperature": {"$avg": "$avg_temperature"},
            "days_with_data": {"$sum": 1}
        }},
        {"$sort": {"overall_avg_temperature": 1}}
    ]

    results = db.weather_reports.aggregate(pipeline)

    logger.info(f"=== Average Temperature between {start_hour}:00 and {end_hour}:00 for data beween {start_date.strftime('%Y-%m-%d')}:00 and {end_date.strftime('%Y-%m-%d')} for Stations within {radius_miles} miles of [{longitude}, {latitude}] ===")
    logger.info(f"\nPipeline for weather_reports.aggregate():")
    logger.info(pprint.pformat(pipeline))
    count = DualLog.log_results(results)
    logger.info(f"Total stations with data: {count}")
    DualLog.log_section_break()


def get_airport_wind_speeds(db, start_date, end_date):
    """
    Query: Get max and min wind speeds for the weather stations at airports in the database, for a period of time

    Args:
        db (Database): The weather database
        start_date (datetime): Start date (inclusive) for the filter
        end_date (datetime): End date (inclusive) for the filter
    """
    
    # Get wind speed data for reports submitted by Airports by using the owner_type as a filter
    pipeline = [
        {"$match": {
            "date": {"$gte": start_date, "$lte": end_date},
            "day_summary.wind_speed_max": {"$exists": True},
            "owner.owner_type": "Airport" 
        }},
        {"$project": {
            "station_id": "$station.station_id",
            "station_name": "$station.name",
            "date": "$date",
            "max_wind_speed": "$day_summary.wind_speed_max",
            "min_wind_speed": "$day_summary.wind_speed_min",
            "mean_wind_speed": "$day_summary.wind_speed_mean"
        }},
        {"$group": {
            "_id": "$station_id",
            "station_name": {"$first": "$station_name"},
            "overall_max_wind_speed": {"$max": "$max_wind_speed"},
            "overall_min_wind_speed": {"$min": "$min_wind_speed"},
            "overall_avg_wind_speed": {"$avg": "$mean_wind_speed"},
            "days_with_data": {"$sum": 1}
        }},
        {"$sort": {"overall_max_wind_speed": -1}}
    ]

    results = db.weather_reports.aggregate(pipeline)
    logger.info(f"=== Wind Speeds for Airport owned Weather Stations from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} ===")
    logger.info(f"\nPipeline for weather_reports.aggregate():")
    logger.info(pprint.pformat(pipeline))
    count = DualLog.log_results(results)    
    logger.info(f"Total stations with wind speed data: {count}")
    DualLog.log_section_break()

def get_technician_activity(db, station_id, days=30):
    """
    Query: Get the ids of technicians that have maintained a weather station in the last month

    Args:
        db (Database): The weather database
        station_id (str): The ID of the weather station
        days (int): Number of days to look back
    """

    # Compute the start date as 'days' ago from now
    start_date = datetime.now() - timedelta(days=180)
   
    # Find technicians from maintenance logs
    pipeline = [
        {"$match": {
            "station_id": station_id,
            "timestamp": {"$gte": start_date}
        }},
        {"$facet": {
            "tech_summary": [
                { "$group": {
                    "_id": "$tech_id",
                    "log_count": { "$sum": 1 }
                }},
                {"$lookup": {
                    "from": "technicians",            # use the technicians collection
                    "localField": "_id",              # _id from previous group (tech_id)
                    "foreignField": "_id",            # Must match tech_id field in technicians
                    "as": "tech_info"
                }},
                {"$project": {
                    "tech_id": "$_id",
                    "log_count": 1,
                    "name": "$tech_info.name",
                    "telephone": "$tech_info.telephone"
                }},
                {"$sort": { "log_count": -1 }
                }
            ],
            "total_tech_count": [
                { "$group": { "_id": "$tech_id" } },
                { "$count": "total_techs" }
            ]}
        }
    ]

    results = db.maintenance_logs.aggregate(pipeline)
    logger.info(f"=== Technicians Who Performed Maintenance in the Last {days} Days ===")
    logger.info(f"\nPipeline for maintenance_logs.aggregate():")
    logger.info(pprint.pformat(pipeline))
    count = DualLog.log_results(results)  
    DualLog.log_section_break()


def get_weather_balloon_readings_by_page(db, station_id, launch_date, page, page_size):
    """
    Query: get readings from a weather balloon launch (sorted by altitude, retrieving a page of readings at a time) 

    Args:
        db (Database): The weather database
        station_id (str): The ID of the ground station
        launch_date (datetime): The date of interest
        page (int): the page of readings to get in this call
        page_size (int): the number of readings in a page
    """

    # Compute the number of readings to skip
    skip = (page-1)*page_size;
    start_date = launch_date
    end_date = launch_date + timedelta(days=1)
   
    # Find the readings of a weather balloon launch and extract a page of temperature values
    pipeline = [
        {"$match": {
            "station.station_id": station_id,
            "launch_date": {
                "$gte": start_date,
                "$lt": end_date
            }
        }},
        {
            "$unwind": "$readings"
        },
        {
            "$sort": {
                "readings.gpheight": 1
            }
        },
        {   "$skip": skip }, 
        {   "$limit": page_size },
        {
            "$project": {
                "_id": 0,
                "timestamp": "$readings.timestamp",
                "gpheight": "$readings.gpheight",
                "temp": "$readings.temp",
            }
        }
    ]

    results = db.weather_balloon_reports.aggregate(pipeline)
    logger.info(f"=== Weather Balloon readings for Station {station_id} on {launch_date.strftime('%Y-%m-%d')}, page {page} ===")
    logger.info(f"\nPipeline for weather_balloon_reports.aggregate():")
    logger.info(pprint.pformat(pipeline))
    count = DualLog.log_results(results)  
    DualLog.log_section_break()    

# build a report that details any day where 3pm is cooler than 9am
# - uses arithmetic features to convert temperature to Fahrenheit
def create_cooler_afternoons_report(station_id, search_year):
    # create an aggregation pipeline to build the cooler afternoon report
    # stages:
    #   1. Filter by station and year
    #   2. Unwind readings array
    #   3. Extract the hour from each reading timestamp
    #   4. Keep the readings at 9AM and 3PM
    #   5. Group by date and report on the 9AM and 3PM readings
    #   6. Calculate temperature difference and compute Fahrenheit version of temperature, rounding the results to 1 decimal place
    #   7. Filter to only keep days where 3PM temp < 9AM temp
    pipeline = [
        {"$match": {
            "station.station_id": station_id,
            "$expr": {
                "$eq": [{ "$year": "$date" }, search_year]
                } } },
        {"$unwind": "$readings"},
        {"$addFields": {
            "reading_hour": { "$hour": "$readings.timestamp" } } },
        {"$match": {
            "reading_hour": { "$in": [9, 15] } } },
        {"$group": {
            "_id": "$date",
            "station_id": { "$first": "$station.station_id" },
            "station_name": { "$first": "$station.name" },
            "temp_9am": { 
                "$max": {
                    "$cond": [{ "$eq": ["$reading_hour", 9] }, "$readings.temp", None]
                } },
            "temp_3pm": {
            "$max": {
                    "$cond": [{ "$eq": ["$reading_hour", 15] }, "$readings.temp", None]
                } } } },
        {"$addFields": {
            "temp_diff": {
                "$round": [
                    { "$subtract": ["$temp_3pm", "$temp_9am"] }, 1 ] },
            "temp_9am_f": {
                "$round": [
                    { "$add": [{ "$multiply": ["$temp_9am", 1.8] }, 32] }, 1 ] },
            "temp_3pm_f": {
                "$round": [
                    { "$add": [{ "$multiply": ["$temp_3pm", 1.8] }, 32] }, 1 ] },
            "temp_diff_f": {
                "$round": [
                    { "$subtract": [{ "$add": [{ "$multiply": ["$temp_3pm", 1.8] }, 32] },
                        { "$add": [{ "$multiply": ["$temp_9am", 1.8] }, 32] } ] }, 1 ] } } },
        {"$match": {
            "$expr": { "$lt": ["$temp_3pm", "$temp_9am"] } } } ]

    results = db.weather_reports.aggregate(pipeline)

    logger.info(f"\n=== Preparing the Cooler Afternoons Report for Station {station_id} for {search_year} year ===")
    logger.info(f"\nPipeline for weather_reports.aggregate():")
    logger.info(pprint.pformat(pipeline))
    DualLog.log_results(results)  
    DualLog.log_section_break()     


# Main script to run Read only test queries on the weather database
if __name__ == "__main__":

    # Initialise a connection to the database server
    client = MongoClient("mongodb://localhost:27017/")
    db_name='weather'
    db = client[db_name]

    # validate the login for a user by comparing userId and password with the users collection
    validate_user(db, "ps2004", "Pa$$1234")
    # now test invalid userid
    validate_user(db, "ps200", "Pa$$1234")
    # now test invalid password
    validate_user(db, "ps2004", "Pa$$123")
    # test institution login which won't require decryption
    validate_user(db, "metoff1", "Pa$$5678")
    # find weather stations within 70 miles of Ipswich
    find_stations_near_location(db, 1.1481, 52.0597, 70)
    # find a weather report for the Norwich weather station on 1st March 2025
    get_weather_report(db, "WS-NOR002", datetime(2025, 3, 1))
    # calculate the average temperature for each day between 10am and 3pm for Norwich
    calculate_average_temperature_time_range(db, "WS-NOR002")
    # calculate the average temperature for each day between 10am and 3pm for Belfast
    calculate_average_temperature_time_range(db, "WS-BEL001")
    # calculate the average temperature for each day between 11am and 2pm for readings within 100 miles of London for Jan 2024
    calculate_average_temperature_area(db, -0.1630249, 51.493847, 100, 11, 14, datetime(2024, 1, 1), datetime(2024, 2, 1))
    # calculate the average temperature for each day between 11am and 2pm for readings within 100 miles of London for Aug 2024
    calculate_average_temperature_area(db, -0.1630249, 51.493847, 100, 11, 14, datetime(2024, 8, 1), datetime(2024, 9, 1))
    # get airport wind speed data for Summer 2024
    get_airport_wind_speeds(db, datetime(2024, 6, 21), datetime(2024, 9, 21))
    # get airport wind speed data for Autumn 2023
    get_airport_wind_speeds(db, datetime(2023, 9, 22), datetime(2023, 12, 20))
    # get technicians that have maintained the Glasgow weather station in the last 180 days
    get_technician_activity(db, "WS-GLA001", 180)
    # get temperature readings from a weather balloon launch (sorted by altitude) first 5 readings
    get_weather_balloon_readings_by_page(db, "03743", datetime(2025, 4, 1), 1, 5)
    # get temperature readings from a weather balloon launch (sorted by altitude) next 5 readings
    get_weather_balloon_readings_by_page(db, "03743", datetime(2025, 4, 1), 2, 5)
    # get days in 2024 when Southampton was cooler at 3pm than 9am
    create_cooler_afternoons_report("WS-SOU001", 2024)

