import csv
import random
import json
from collections import defaultdict
from datetime import datetime, timedelta
from pymongo import MongoClient, ASCENDING, DESCENDING, GEOSPHERE
from helper import DualLog, DateHelper, DateTimeEncoder, MongoDBHandler
from model import WeatherStationReading, WeatherStationDaySummary, Location, LocationWithAltitude
from model import RadiosondeReading, GroundStation, WeatherBalloonReport, WeatherCategory, WeatherObservation, WeatherReport
from model import WeatherStation, Institution, WeatherWatcher, Administrator, Technician, MaintenanceLogItem

# Initialise the logger
logger = DualLog.create_log("build_database_and_import_data.log")

# Creates synthetic maintenance log test data
class RandomMaintenanceLogGenerator:    
    def generate_maintenance_logs(self, station_id, technicians, start_date, end_date):
        # Pool of potential report items
        reports = [
            "No adjustments required",
            "Test log entry added.",
            "Performed full diagnostics.",
            "Calibrated temperature sensor.",
            "Replaced faulty anemometer.",
            "Cleaned solar panel.",
            "Reset communication module.",
            "Battery backup replaced.",
            "Tested humidity sensor.",
            "Checked rain gauge calibration.",
            "Cleared debris from sensor mount.",
            "Verified data transmission.",
            "Firmware updated.",
            "Sensor alignment adjusted.",
            "Power cycle performed.",
            "Secured loose wiring.",
            "Removed bird nesting material.",
            "Cleared obstruction from wind vane.",
            "Checked solar charging circuit.",
            "Repaired housing.",
            "Replaced wiring."
        ]

        maintenance_logs = []

        current_date = start_date
        while current_date <= end_date:
            tech = random.choice(technicians)
            hour = random.randint(9, 16)
            minute = random.randint(0, 59)
            timestamp = datetime(current_date.year, current_date.month, current_date.day, hour, minute)
            report = random.choice(reports)
            log_item = MaintenanceLogItem(timestamp, station_id, tech.tech_id, report)
            maintenance_logs.append(log_item)
            current_date += timedelta(days=random.randint(5, 10)) # next maintenance will be in 5 to 10 days time

        return maintenance_logs


# Process the CSV files previously downlaoded from Open-meteo.com 
#
# CSV file from open-meteo.com is in three sections
# Each section has a header line and is separated from the next section by a blank line
# Sections:
# 1) A single line holding the location of the weather station
# 2) Hourly weather readings
# 3) Daily weather summaries
class OpenMeteoWeatherDataLoader:
    def __init__(self, station_id, station_name, owner, technicians):
        self.station_id = station_id
        self.station_name = station_name
        self.owner = owner
        self.technicians = technicians
        
    def load_from_csv(self, filename):
        daily_readings = defaultdict(list)

        with open(filename, newline='') as csvfile:
            lines = csvfile.read().splitlines()

        # Split into sections by blank lines
        sections = []
        current = []
        for line in lines:
            if line.strip() == "":
                if current:
                    sections.append(current)
                    current = []
            else:
                current.append(line)
        if current:
            sections.append(current)

        # Process weather station location data (first 2 lines: header + data)
        station_data_header = sections[0][0].split(",")
        station_data_values = sections[0][1].split(",")
        self.station_data = dict(zip(station_data_header, station_data_values))
        location = Location.from_csv_data(self.station_data)

        # Process hourly data
        reader = csv.DictReader(sections[1])
        daily_readings = defaultdict(list) # will create a new list for each day in the dictionary when the first reading is appended
        one_hour = 3600 # in seconds
        for row in reader:
            reading = WeatherStationReading.from_csv_data(row, one_hour)
            day = reading.timestamp.date() # extract the date without the time
            daily_readings[day].append(reading)

        # Process daily summaries
        summaries = {}
        reader = csv.DictReader(sections[2])
        for row in reader:
            summary = WeatherStationDaySummary.from_csv_data(row) # build the day summary from the row of data
            summaries[summary.date.date()] = summary # store it using just the date so that it matches 'day' used to store readings

        if (self.technicians is not None):
            # Generate some random maintenance logs
            logGen = RandomMaintenanceLogGenerator()
            weather_station_maintenance_reports = logGen.generate_maintenance_logs(self.station_id, self.technicians, datetime(2023, 1, 1), datetime(2025, 4, 9))
            latest_maintenance = weather_station_maintenance_reports[-1] # most recent maintenance report
        else:
            weather_station_maintenance_reports = None
            latest_maintenance = None

        # Build the WeatherStation report
        weather_station_report = WeatherStation(self.station_id, self.station_name, location.to_geojson(), self.owner, "Online", latest_maintenance) 

        # Combine all into WeatherReport
        weather_reports = []
        for date, readings in daily_readings.items():
            summary = summaries.get(date)
            weather_reports.append(WeatherReport(summary.date, location.to_geojson(), DateHelper.get_end_of_day(summary.date), 1, weather_station_report, owner=self.owner, readings=readings, day_summary=summary.get_doc_for_weather_report()))

        return weather_reports, weather_station_report, weather_station_maintenance_reports


# class to import CSV data from Open-Meteo into the database
class OpenMeteoDataImporter:
    def __init__(self, mongo_handler):
        self.mongo_handler = mongo_handler

    def import_data(self, station_id, station_name, filename, owner, technicians):
        logger.info(f"\n=== Importing {filename} for Weather Station {station_id} {station_name} ===")
        loader = OpenMeteoWeatherDataLoader(station_id, station_name, owner, technicians)
        weather_reports, weather_station_report, weather_station_maintenance_reports = loader.load_from_csv(filename)

        collection_weather_reports = mongo_handler.get_collection('weather_reports')
        documents_weather_reports = [day_data.get_weather_report_doc() for day_data in weather_reports]
        title = f"\nUsing insert_many() to add to the weather_reports collection."
        self.mongo_handler.insert_many(collection_weather_reports, documents_weather_reports, logger, title, log_first_document=True)

        collection_weather_stations = mongo_handler.get_collection('weather_stations')
        document_weather_station = weather_station_report.get_weather_station_doc()
        title = f"\nUsing insert_one() to add to the weather_stations collection."
        self.mongo_handler.insert_one(collection_weather_stations, document_weather_station, logger)

        if (weather_station_maintenance_reports is not None):
            collection_maintenance_logs = mongo_handler.get_collection('maintenance_logs')
            documents_maintenance_logs = [log_item.get_maintenance_log_doc() for log_item in weather_station_maintenance_reports]
            title = f"\nUsing insert_many() to add to the maintenance_logs collection."
            self.mongo_handler.insert_many(collection_maintenance_logs, documents_maintenance_logs, logger, title, log_first_document=True)

        DualLog.log_section_break()

    def create_test_weather_reports_from_csv(self, station_id, station_name, filename, owner, technicians):
        loader = OpenMeteoWeatherDataLoader(station_id, station_name, owner, technicians)
        weather_reports, weather_station_report, weather_station_maintenance_reports = loader.load_from_csv(filename)
        documents_weather_reports = [day_data.get_weather_report_doc() for day_data in weather_reports]
        print(f"JSON document for extra day of data for {station_name}")
        print(json.dumps(documents_weather_reports, cls=DateTimeEncoder))

# class to import and convert Radiosonde JSON data from Windy.com into the database
class RadiosondeDataImporter:
    def __init__(self, mongo_handler):
        self.mongo_handler = mongo_handler

    def import_data(self, launch_date, ground_station_id, ground_station_name, filename, owner, log_document=False):
        logger.info(f"\n=== Importing {filename} for Weather Balloon launched from Ground Station {ground_station_id} {ground_station_name} ===")

        # Load the JSON file
        with open(filename, "r", encoding="utf-8") as f:
            json_data = json.load(f)

        # Convert features to list of RadiosondeReading instances        
        readings = []
        first_location = None
        for feature in json_data["features"]:
            geometry = feature["geometry"]
            # ignore the feature if the geometry is anything other than a Point
            if (geometry["type"] == 'Point'):
                props = feature["properties"]

                location = LocationWithAltitude.from_geojson(geometry)
                reading = RadiosondeReading.from_geojson_properties(props, location)
                readings.append(reading)

                if (first_location is None):
                    first_location = reading.location
                last_modified = reading.timestamp
            
        top_properties = json_data["properties"]
        swversion = top_properties["sonde_swversion"]
        serial = top_properties["sonde_serial"]

        # create the Ground Station 
        ground_station = GroundStation(ground_station_id, ground_station_name, owner)

        # create the Weather Balloon Report 
        weather_balloon_report = WeatherBalloonReport(launch_date, ground_station, first_location, serial, swversion, last_modified, 1, readings)

        collection_weather_balloon_reports = mongo_handler.get_collection('weather_balloon_reports')
        document_weather_reports = weather_balloon_report.get_weather_balloon_report_doc()
        title = f"\nUsing insert_one() to add the document to the weather_balloon_reports collection."
        self.mongo_handler.insert_one(collection_weather_balloon_reports, document_weather_reports, logger, title, log_document)
    

# class to Drop the existing weather database and builds a new one from scratch
class DatabaseBuilder:
    def __init__(self, client, db_name):
        # Check if the database exists and drop it
        if db_name in client.list_database_names():
            client.drop_database(db_name)

        # Create the DB
        logger.info(f"=== Create database '{db_name}' ===")
        db = client[db_name]

        # Create the collections
        logger.info(f"\n=== Create collections ===")
        self.collection_users = db.create_collection("users")
        self.collection_technicians = db.create_collection("technicians")
        self.collection_weather_reports = db.create_collection("weather_reports")
        self.collection_weather_stations = db.create_collection("weather_stations")
        self.collection_maintenance_logs = db.create_collection("maintenance_logs")
        self.collection_weather_balloon_reports = db.create_collection("weather_balloon_reports")

        # Get collection names
        collection_names = db.list_collection_names()

        # Log them
        logger.info("\nCollections in the database:")
        for name in collection_names:
            logger.info(f"- {name}")

        DualLog.log_section_break()

        # Create the indexes
        logger.info(f"\n=== Create indexes ===")
        logger.info(f"Adding indexes to weather_reports collection")
        self.collection_weather_reports.create_index([("station.station_id", ASCENDING)])
        self.collection_weather_reports.create_index([("date", DESCENDING)])
        self.collection_weather_reports.create_index([("location", GEOSPHERE)])
        self.collection_weather_reports.create_index([("readings.timestamp", DESCENDING)])
        self.collection_weather_reports.create_index([("owner.owner_type", ASCENDING)])
        logger.info(f"Adding indexes to weather_stations collection")
        self.collection_weather_stations.create_index([("location", GEOSPHERE)])
        self.collection_weather_stations.create_index([("owner.owner_type", ASCENDING)])
        self.collection_weather_stations.create_index([("status", ASCENDING)])
        logger.info(f"Adding indexes to weather_balloon_reports collection")
        self.collection_weather_balloon_reports.create_index([("location", GEOSPHERE)])
        self.collection_weather_balloon_reports.create_index([("readings.location", GEOSPHERE)])
        self.collection_weather_balloon_reports.create_index([("launch_date", DESCENDING)])
        self.collection_weather_balloon_reports.create_index([("station.station_id", ASCENDING)])
        logger.info(f"Adding indexes to maintenance_logs collection")
        self.collection_maintenance_logs.create_index([("timestamp", DESCENDING)])
        self.collection_maintenance_logs.create_index([("tech_id", ASCENDING)])
        
        logger.info(f"\n=== List of indexes ===")
        # List and log the indexes
        for coll_name in collection_names:
            logger.info(f"Collection: {coll_name}")
            collection = db[coll_name]
            indexes = collection.list_indexes()
            for index in indexes:
                logger.info(f"   Index: {index['name']}")
                logger.info(f"   Keys: {list(index['key'].items())}")
            logger.info("")
    
    def populateUsers(self, test_data, mongo_handler):
        logger.info(f"\n=== Insert users into the users collection ===")
        
        title = f"\nUsing insert_one() to add to the admin user to the users collection."
        mongo_handler.insert_one(self.collection_users, test_data.admin_user.get_user_doc(), logger, title, log_document = True)
        
        title = f"\nUsing insert_one() to add each institution to the users collection."
        mongo_handler.insert_one(self.collection_users, test_data.inst_met_office.get_user_doc(), logger, title, log_document = True)
        mongo_handler.insert_one(self.collection_users, test_data.inst_heathrow.get_user_doc(), logger, "\n", log_document = True)
        mongo_handler.insert_one(self.collection_users, test_data.inst_gatwick.get_user_doc(), logger, "\n", log_document = True)
        mongo_handler.insert_one(self.collection_users, test_data.inst_cambridge_uni.get_user_doc(), logger, "\n", log_document = True)
        title = f"\nUsing insert_one() to add each weather watcher to the users collection."
        mongo_handler.insert_one(self.collection_users, test_data.ww_paul.get_user_doc(), logger, title, log_document = True)
        mongo_handler.insert_one(self.collection_users, test_data.ww_simon.get_user_doc(), logger, "\n", log_document = True)
        DualLog.log_section_break()

    def populateTechnicians(self, test_data, mongo_handler):
        logger.info(f"\n=== Insert technicians into the technicians collection ===")

        tech_docs = [tech.get_technician_doc() for tech in test_data.techs]
        title = f"\nUsing insert_many() to add to the technicians collection."
        mongo_handler.insert_many(self.collection_technicians, tech_docs, logger, title, log_all_documents=True, log_section_break=True)

    def populateReportsForWeatherObservations(self, test_data, mongo_handler):
        logger.info(f"\n=== Insert weather reports for manual Weather Observations ===")
        title = f"\nUsing insert_one() to add a weather report (with observation and photo) into the weather_reports collection."
        mongo_handler.insert_one(self.collection_weather_reports, test_data.manualReport1.get_weather_report_doc(), logger, title)
        mongo_handler.insert_one(self.collection_weather_reports, test_data.manualReport2.get_weather_report_doc(), logger, title)
        mongo_handler.insert_one(self.collection_weather_reports, test_data.manualReport3.get_weather_report_doc(), logger, title, log_section_break=True)


# Creates synthetic test data to support the imported weather data
class TestDataGenerator:
    def __init__(self):
        # create database admin user test data
        self.admin_user = Administrator("admin1","Pa$$4422","John Smith", "j.smith@weatherdb.com")

        # create institution test data
        self.inst_met_office = Institution("metoff1","Pa$$5678","Met Office", "Government", "Mr Simon Jones", "stations@metoffice.gov.uk", "03709000100")
        self.inst_heathrow = Institution("heathAir","Pa$$1234","Heathrow Airport", "Airport", "Mrs Jill Brown", "weather@heathrow.com", "03443351801")
        self.inst_gatwick = Institution("gatAir","Pa$$1234","Gatwick Airport", "Airport", "Mr Peter Smith", "weather@gatwickairport.com", "01293505000")
        self.inst_cambridge_uni = Institution("camUni","Pa$$1234","Cambridge University", "University", "Mr Andy Hopper", "weatherlab@cam.ac.uk", "01223763500")

        # create weather watcher test data
        self.ww_paul = WeatherWatcher("ps2004","Pa$$1234","Paul S","Paul Smithers","pauls@gmail.com")
        self.ww_simon = WeatherWatcher("sp_hawk","Pa$$1234","Simon","Simon Peters","peters27@outlook.com")
        
        # create weather station technician test data
        self.tech1 = Technician("T-001", "John Smith", "AtmosTech Ltd", "contact@atmostech.co.uk", "02079460011")
        self.tech2 = Technician("T-002", "Emily Carter", "AtmosTech Ltd", "contact@atmostech.co.uk", "02079460011")
        self.tech3 = Technician("T-003", "Liam Turner", "ClimaServ UK", "tech@climaserv.com", "01618504403")
        self.tech4 = Technician("T-004", "Sophie Bennett", "ClimaServ UK", "tech@climaserv.com", "01618504403")
        self.tech5 = Technician("T-005", "Oliver White", "MeteoCare Solutions", "enquiries@meteocare.co.uk", "01134968801")
        self.tech6 = Technician("T-006", "Gary Mitchell", "MeteoCare Solutions", "enquiries@meteocare.co.uk", "01134968801")
        self.tech7 = Technician("T-007", "Harry Wilson", "ClimaServ UK", "tech@climaserv.com", "01618504403")
        self.tech8 = Technician("T-008", "Charlotte Green", "AtmosTech Ltd", "contact@atmostech.co.uk", "02079460011")

        self.techs = [self.tech1, self.tech2, self.tech3, self.tech4, self.tech5, self.tech6, self.tech7, self.tech8]

        self.observ1 = WeatherObservation(datetime(2025, 2, 26, 14, 12), WeatherCategory.HEAVY_RAIN, 10, "Heavy rain caused flooding", photo_filename="images/flood.jpg")
        self.manualReport1 = WeatherReport(datetime(2025, 2, 26), Location(52.09332, 1.32042).to_geojson(), self.observ1.timestamp, 1, owner=self.ww_paul, observations=[self.observ1])
        self.observ2 = WeatherObservation(datetime(2025, 2, 24, 10, 53), WeatherCategory.LIGHT_SHOWERS, 11, "Double rainbow", wind_speed=5, wind_direction=205, photo_filename="images/double_rainbow.jpg")
        self.manualReport2 = WeatherReport(datetime(2025, 2, 24), Location(52.18893, 0.99774).to_geojson(), self.observ2.timestamp, 1, owner=self.ww_simon, observations=[self.observ2])
        self.observ3 = WeatherObservation(datetime(2024, 11, 28, 8, 35), WeatherCategory.SUNNY_INTERVALS, 1, "Cold frosty morning", precip=0, sample_duration=3600, photo_filename="images/morning_frost.jpg")
        self.manualReport3 = WeatherReport(datetime(2024, 11, 28), Location(52.198101, 1.031860).to_geojson(), self.observ3.timestamp, 1, owner=self.ww_simon, observations=[self.observ3])


# Main script to build and populate the weather database
if __name__ == "__main__":
    
    # seed the random number generator to create consistent data on each run
    random.seed(99)

    # Initialise a connection to the database server
    client = MongoClient("mongodb://localhost:27017/")
    db_name='weather'

    # Build the Database
    builder = DatabaseBuilder(client, db_name)

    # Create Test Data
    test_data = TestDataGenerator()

    # Initialise MongoDB handler for using the database
    mongo_handler = MongoDBHandler(client, db_name)

    # Populate the Users collection
    builder.populateUsers(test_data, mongo_handler)

    # Populate the Technicians collection
    builder.populateTechnicians(test_data, mongo_handler)

    # Populate the database with CSV data
    importer = OpenMeteoDataImporter(mongo_handler)
    # met office weather stations
    importer.import_data("WS-BEL001", "Belfast", "data/OpenMeteo/open-meteo-Belfast.csv", test_data.inst_met_office, [test_data.tech1])
    importer.import_data("WS-BIR001", "Birmingham", "data/OpenMeteo/open-meteo-Birmingham.csv", test_data.inst_met_office, [test_data.tech2])
    importer.import_data("WS-BRI005", "Bristol", "data/OpenMeteo/open-meteo-Bristol.csv", test_data.inst_met_office, [test_data.tech7, test_data.tech8])
    importer.import_data("WS-EDI003", "Edinburgh", "data/OpenMeteo/open-meteo-Edinburgh.csv", test_data.inst_met_office, [test_data.tech4, test_data.tech5])
    importer.import_data("WS-GLA001", "Glasgow", "data/OpenMeteo/open-meteo-Glasgow.csv", test_data.inst_met_office, [test_data.tech2, test_data.tech3])
    importer.import_data("WS-LEE002", "Leeds", "data/OpenMeteo/open-meteo-Leeds.csv", test_data.inst_met_office, [test_data.tech4])
    importer.import_data("WS-LIV003", "Liverpool", "data/OpenMeteo/open-meteo-Liverpool.csv", test_data.inst_met_office, [test_data.tech3, test_data.tech4])
    importer.import_data("WS-LON002", "London", "data/OpenMeteo/open-meteo-London.csv", test_data.inst_met_office, [test_data.tech6, test_data.tech7])
    importer.import_data("WS-MAN004", "Manchester", "data/OpenMeteo/open-meteo-Manchester.csv", test_data.inst_met_office, [test_data.tech2, test_data.tech3])
    importer.import_data("WS-NEW002", "Newcastle", "data/OpenMeteo/open-meteo-Newcastle.csv", test_data.inst_met_office, [test_data.tech4, test_data.tech5])
    importer.import_data("WS-NOR002", "Norwich", "data/OpenMeteo/open-meteo-Norwich.csv", test_data.inst_met_office, [test_data.tech6])
    importer.import_data("WS-SOU001", "Southampton", "data/OpenMeteo/open-meteo-Southampton.csv", test_data.inst_met_office, [test_data.tech7, test_data.tech8])
    importer.import_data("WS-SWA003", "Swansea", "data/OpenMeteo/open-meteo-Swansea.csv", test_data.inst_met_office, [test_data.tech7, test_data.tech8])
    # Airport/University weather stations
    importer.import_data("WS-AIR001", "Heathrow", "data/OpenMeteo/open-meteo-Heathrow.csv", test_data.inst_heathrow, [test_data.tech6])
    importer.import_data("WS-AIR002", "Gatwick", "data/OpenMeteo/open-meteo-Gatwick.csv", test_data.inst_gatwick, [test_data.tech7])
    importer.import_data("WS-UNI001", "Cambridge", "data/OpenMeteo/open-meteo-Cambridge.csv", test_data.inst_cambridge_uni, [test_data.tech6])
    # Private weather station
    importer.import_data("WS-WW1001", "Ipswich", "data/OpenMeteo/open-meteo-Ipswich.csv", test_data.ww_paul, None)

    # Populate the database with Weather Balloon Reports (write the first one to the log)
    radiosonde_importer = RadiosondeDataImporter(mongo_handler)
    radiosonde_importer.import_data(datetime(2025, 4, 1, 8, 0), "03743", "Larkhill", "data/Windy/03743_20250401_080000.geojson", test_data.inst_met_office, True)
    radiosonde_importer.import_data(datetime(2025, 4, 3, 8, 0), "03743", "Larkhill", "data/Windy/03743_20250403_080000.geojson", test_data.inst_met_office)
    radiosonde_importer.import_data(datetime(2025, 4, 8, 8, 0), "03743", "Larkhill", "data/Windy/03743_20250408_080000.geojson", test_data.inst_met_office)
    radiosonde_importer.import_data(datetime(2025, 4, 4, 23, 0), "03354", "Nottingham", "data/Windy/03354_20250404_230000.geojson", test_data.inst_met_office)
    radiosonde_importer.import_data(datetime(2025, 4, 6, 23, 0), "03354", "Nottingham", "data/Windy/03354_20250406_230000.geojson", test_data.inst_met_office)
    radiosonde_importer.import_data(datetime(2025, 4, 8, 23, 0), "03354", "Nottingham", "data/Windy/03354_20250408_230000.geojson", test_data.inst_met_office)
    radiosonde_importer.import_data(datetime(2025, 4, 6, 11, 0), "03808", "Cambourne", "data/Windy/03808_20250406_110000.geojson", test_data.inst_met_office)
    radiosonde_importer.import_data(datetime(2025, 4, 7, 11, 0), "03808", "Cambourne", "data/Windy/03808_20250407_110000.geojson", test_data.inst_met_office)
    radiosonde_importer.import_data(datetime(2025, 4, 8, 11, 0), "03808", "Cambourne", "data/Windy/03808_20250408_110000.geojson", test_data.inst_met_office)

    # Add Weather Reports for manual Weather Observations with photos
    builder.populateReportsForWeatherObservations(test_data, mongo_handler)



    
