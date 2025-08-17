"""
The data model of the database.
Contains classes for each of the main entities and many of the sub-objects
that are embedded into the documents.
They have methods to form the data in the correct format for the documents based 
on the physical model.
Some classes have methods to populate themselves from the source data attributes.
"""

import math
from typing import Optional
from enum import Enum
from datetime import datetime
from bson.binary import Binary
from encrypt import EncryptionHelper
from helper import  DateHelper

class WeatherStationReading:
    def __init__(self, timestamp, sample_duration, temp, dewpoint, humidity, pressure,
                 precip, cloud_cover, wind_speed, wind_direction, sunshine,
                 soil_0_7_temp, soil_0_7_moisture,
                 soil_7_28_temp, soil_7_28_moisture,
                 soil_28_100_temp, soil_28_100_moisture,
                 soil_100_255_temp, soil_100_255_moisture):
        
        self.timestamp = timestamp
        self.sample_duration = sample_duration
        self.temp = temp
        self.dewpoint = dewpoint
        self.humidity = humidity
        self.pressure = pressure
        self.precip = precip
        self.cloud_cover = cloud_cover
        self.wind_speed = wind_speed
        self.wind_direction = wind_direction
        self.sunshine = sunshine

        self.soil_0_7_temp = soil_0_7_temp
        self.soil_0_7_moisture = soil_0_7_moisture
        self.soil_7_28_temp = soil_7_28_temp
        self.soil_7_28_moisture = soil_7_28_moisture
        self.soil_28_100_temp = soil_28_100_temp
        self.soil_28_100_moisture = soil_28_100_moisture
        self.soil_100_255_temp = soil_100_255_temp
        self.soil_100_255_moisture = soil_100_255_moisture

    @classmethod
    def from_csv_data(cls, row, sample_duration):
        return cls(
            timestamp=DateHelper.parse_csv_datetime(row['time']),
            sample_duration=sample_duration,
            temp=float(row['temperature_2m (Â°C)']),
            dewpoint=float(row['dew_point_2m (Â°C)']),
            humidity=float(row['relative_humidity_2m (%)']),
            pressure=float(row['pressure_msl (hPa)']),
            precip=float(row['precipitation (mm)']),
            cloud_cover=float(row['cloud_cover (%)']),
            wind_speed=float(row['wind_speed_10m (m/s)']),
            wind_direction=float(row['wind_direction_10m (Â°)']),
            sunshine=float(row['sunshine_duration (s)']),
            soil_0_7_temp=float(row['soil_temperature_0_to_7cm (Â°C)']),
            soil_0_7_moisture=float(row['soil_moisture_0_to_7cm (mÂ³/mÂ³)']),
            soil_7_28_temp=float(row['soil_temperature_7_to_28cm (Â°C)']),
            soil_7_28_moisture=float(row['soil_moisture_7_to_28cm (mÂ³/mÂ³)']),
            soil_28_100_temp=float(row['soil_temperature_28_to_100cm (Â°C)']),
            soil_28_100_moisture=float(row['soil_moisture_28_to_100cm (mÂ³/mÂ³)']),
            soil_100_255_temp=float(row['soil_temperature_100_to_255cm (Â°C)']),
            soil_100_255_moisture=float(row['soil_moisture_100_to_255cm (mÂ³/mÂ³)'])
        )

    # create a document containing the data in a suitable format for embedding in a weather_report
    def get_doc_for_weather_report(self):
        return {
            "timestamp": self.timestamp,
            "sample_duration": self.sample_duration,
            "temp": self.temp,
            "dewpoint": self.dewpoint,
            "humidity": self.humidity,
            "pressure": self.pressure,
            "precip": self.precip,
            "cloud_cover": self.cloud_cover,
            "wind_speed": self.wind_speed,
            "wind_direction": self.wind_direction,
            "sunshine": self.sunshine,
            "soil": {
                "0_to_7cm": {
                    "temp": self.soil_0_7_temp,
                    "moisture": self.soil_0_7_moisture
                },
                "7_to_28cm": {
                    "temp": self.soil_7_28_temp,
                    "moisture": self.soil_7_28_moisture
                },
                "28_to_100cm": {
                    "temp": self.soil_28_100_temp,
                    "moisture": self.soil_28_100_moisture
                },
                "100_to_255cm": {
                    "temp": self.soil_100_255_temp,
                    "moisture": self.soil_100_255_moisture
                }
            }
        }


class WeatherStationDaySummary:
    def __init__(self, date, temp_mean, temp_min, temp_max,
                 dewpoint_mean, humidity_mean, humidity_min, humidity_max,
                 pressure_mean, pressure_min, pressure_max,
                 precip_sum, cloud_cover_mean,
                 wind_speed_mean, wind_speed_min, wind_speed_max,
                 sunshine):
        self.date = date
        self.temp_mean = temp_mean
        self.temp_min = temp_min
        self.temp_max = temp_max
        self.dewpoint_mean = dewpoint_mean
        self.humidity_mean = humidity_mean
        self.humidity_min = humidity_min
        self.humidity_max = humidity_max
        self.pressure_mean = pressure_mean
        self.pressure_min = pressure_min
        self.pressure_max = pressure_max
        self.precip_sum = precip_sum
        self.cloud_cover_mean = cloud_cover_mean
        self.wind_speed_mean = wind_speed_mean
        self.wind_speed_min = wind_speed_min
        self.wind_speed_max = wind_speed_max
        self.sunshine = sunshine

    @classmethod
    def from_csv_data(cls, row):
        return cls(
            date=DateHelper.parse_csv_date(row['time']),
            temp_mean=float(row['temperature_2m_mean (Â°C)']),
            temp_min=float(row['temperature_2m_min (Â°C)']),
            temp_max=float(row['temperature_2m_max (Â°C)']),
            dewpoint_mean=float(row['dew_point_2m_mean (Â°C)']),
            humidity_mean=float(row['relative_humidity_2m_mean (%)']),
            humidity_min=float(row['relative_humidity_2m_min (%)']),
            humidity_max=float(row['relative_humidity_2m_max (%)']),
            pressure_mean=float(row['pressure_msl_mean (hPa)']),
            pressure_min=float(row['pressure_msl_min (hPa)']),
            pressure_max=float(row['pressure_msl_max (hPa)']),
            precip_sum=float(row['precipitation_sum (mm)']),
            cloud_cover_mean=float(row['cloud_cover_mean (%)']),
            wind_speed_mean=float(row['wind_speed_10m_mean (m/s)']),
            wind_speed_min=float(row['wind_speed_10m_min (m/s)']),
            wind_speed_max=float(row['wind_speed_10m_max (m/s)']),
            sunshine=float(row['sunshine_duration (s)'])
        )        
    
    # create a document containing the data in a suitable format for embedding in a weather_report
    def get_doc_for_weather_report(self):
        doc = {
            'temp_mean': self.temp_mean,
            'temp_min': self.temp_min,
            'temp_max': self.temp_max,
            'dewpoint_mean': self.dewpoint_mean,
            'humidity_mean': self.humidity_mean,
            'humidity_min': self.humidity_min,
            'humidity_max': self.humidity_max,
            'pressure_mean': self.pressure_mean,
            'pressure_min': self.pressure_min,
            'pressure_max': self.pressure_max,
            'precip_sum': self.precip_sum,
            'cloud_cover_mean': self.cloud_cover_mean,
            'wind_speed_mean': self.wind_speed_mean,
            'wind_speed_min': self.wind_speed_min,
            'wind_speed_max': self.wind_speed_max,
            'sunshine': self.sunshine
        }
        return doc

class Location:
    def __init__(self, latitude, longitude):
        self.latitude = latitude
        self.longitude = longitude

    @classmethod
    def from_csv_data(cls, station_dict):
        latitude = float(station_dict['latitude'])
        longitude = float(station_dict['longitude'])
        return cls(latitude, longitude)

    def to_geojson(self):
        return {
            "type": "Point",
            "coordinates": [self.longitude, self.latitude]
        }
    
class LocationWithAltitude:
    def __init__(self, latitude, longitude, altitude):
        self.latitude = latitude
        self.longitude = longitude
        self.altitude = altitude

    @classmethod
    def from_geojson(cls, geometry):
        if geometry["type"] != "Point":
            raise ValueError("Only Point geometries are supported")
        
        coordinates = geometry["coordinates"]
        if len(coordinates) != 3:
            raise ValueError("Expected 3D coordinates [lon, lat, alt]")

        longitude, latitude, altitude = coordinates
        return cls(latitude=latitude, longitude=longitude, altitude=altitude)

    def to_geojson(self):
        return {
            "type": "Point",
            "coordinates": [self.longitude, self.latitude, self.altitude]
        }
    
class RadiosondeReading:
    def __init__(self, timestamp, location : LocationWithAltitude, gpheight, temp, dewpoint, pressure,
                 wind_speed, wind_direction):
        
        self.timestamp = timestamp
        self.location = location
        self.gpheight = gpheight
        self.temp = temp
        self.dewpoint = dewpoint
        self.pressure = pressure
        self.wind_speed = wind_speed
        self.wind_direction = wind_direction

    @classmethod
    def from_geojson_properties(cls, properties, location : LocationWithAltitude):
        unix_timestamp = properties["time"]
        timestamp = datetime.fromtimestamp(unix_timestamp)
        gpheight = properties["gpheight"]
        temp = properties["temp"] - 273.15  # convert from Kelvin to Celsius
        dewpoint = properties["dewpoint"] - 273.15 # convert from Kelvin to Celsius
        pressure = properties["pressure"]
        wind_speed = (properties["wind_u"]**2 + properties["wind_v"]**2)**0.5  # calculate wind speed from u and v components

        # Calculate the wind direction in radians
        wind_direction_radians = math.atan2(properties["wind_v"], properties["wind_u"])
        # Convert radians to degrees
        wind_direction_degrees = math.degrees(wind_direction_radians)
        # restrict to 0 to 360 degree range
        wind_direction_degrees = (wind_direction_degrees + 360) % 360
        
        return cls(timestamp=timestamp, location=location, gpheight=gpheight, temp=temp, dewpoint=dewpoint,
                   pressure=pressure, wind_speed=wind_speed, wind_direction=wind_direction_degrees)

    # create a document containing the data in a suitable format for embedding in a weather_balloon_report
    def get_doc_for_weather_balloon_report(self):
        return {
            "timestamp": self.timestamp,
            "location": self.location.to_geojson(),
            "gpheight": self.gpheight,
            "temp": self.temp,
            "dewpoint": self.dewpoint,
            "pressure": self.pressure,
            "wind_speed": self.wind_speed,
            "wind_direction": self.wind_direction
        }

class GroundStation:
    def __init__(self, 
                 station_id,
                 name, 
                 owner):
        self.station_id = station_id
        self.name = name
        self.owner = owner
    
    # create a document containing the data in a suitable format for embedding in a weather_balloon_report    
    def get_doc_for_weather_balloon_report(self):
        doc = {"station_id": self.station_id,
               "name": self.name,
               "owner": self.owner.get_subset_for_weather_station()}
        return doc

class WeatherBalloonReport:
    def __init__(self, 
                 launch_date,
                 station : GroundStation, 
                 location : LocationWithAltitude, # first detected location
                 radiosonde_serial,
                 radiosonde_software,
                 last_modified,
                 version = 1,                 
                 readings = None):
        self.launch_date = launch_date
        self.station = station
        self.location = location
        self.radiosonde_serial = radiosonde_serial
        self.radiosonde_software = radiosonde_software
        self.last_modified = last_modified
        self.version = version
        self.readings = readings

    # create a document in a suitable format for the weather_balloon_reports collection
    def get_weather_balloon_report_doc(self):
        doc = {"launch_date": self.launch_date,
               "station": self.station.get_doc_for_weather_balloon_report(),
               "location": self.location.to_geojson(),
               "version": self.version,
               "last_modified": self.last_modified,
               "radiosonde": {
                   "serial": self.radiosonde_serial,
                   "software": self.radiosonde_software
               }}
        
        if self.readings is not None:
            doc["readings"] = [r.get_doc_for_weather_balloon_report() for r in self.readings]
           
        return doc

    
class WeatherCategory(Enum):
    CLEAR = "Clear"
    SUNNY = "Sunny"
    SUNNY_INTERVALS = "Sunny intervals"
    LIGHT_CLOUD = "Light cloud"
    HEAVY_CLOUD = "Heavy cloud"
    DRIZZLE = "Drizzle"
    SUNSHINE_AND_SHOWERS = "Sunshine and showers"
    LIGHT_SHOWERS = "Light showers"
    HEAVY_SHOWERS = "Heavy showers"
    LIGHT_RAIN = "Light rain"
    HEAVY_RAIN = "Heavy rain"
    THUNDER_STORM = "Thunder storm"
    THUNDERY_SHOWERS = "Thundery showers"
    SLEET_SHOWERS = "Sleet showers"
    SLEET = "Sleet"
    LIGHT_SNOW_SHOWERS = "Light snow showers"
    HEAVY_SNOW_SHOWERS = "Heavy snow showers"
    LIGHT_SNOW = "Light snow"
    HEAVY_SNOW = "Heavy snow"
    HAIL_SHOWERS = "Hail showers"
    HAIL = "Hail"
    FOG = "Fog"
    HAZY = "Hazy"
    MIST = "Mist"

class WeatherObservation:
    def __init__(self, 
                 timestamp, 
                 category: Optional[WeatherCategory] = None,
                 temp=None, 
                 description=None, 
                 humidity=None, 
                 precip=None, 
                 sample_duration=None, 
                 pressure=None, 
                 wind_speed=None, 
                 wind_direction=None, 
                 photo_filename=None):
        
        if category is not None and not isinstance(category, WeatherCategory):
            raise ValueError(f"category must be one of {list(WeatherCategory)} or None")

        self.timestamp = timestamp
        self.category = category
        self.temp = temp
        self.description = description
        self.humidity = humidity
        self.precip = precip
        self.sample_duration = sample_duration
        self.pressure = pressure
        self.wind_speed = wind_speed
        self.wind_direction = wind_direction
        self.photo_filename = photo_filename

    # create a document containing the data in a suitable format for embedding in a weather_report    
    def get_doc_for_weather_report(self):
        doc = {"timestamp": self.timestamp}

        # Only add fields if they are not None
        if self.category is not None:
            doc["category"] = self.category.value
        if self.temp is not None:
            doc["temp"] = self.temp
        if self.description is not None:
            doc["description"] = self.description
        if self.humidity is not None:
            doc["humidity"] = self.humidity            
        if self.precip is not None:
            doc["precip"] = self.precip
        if self.sample_duration is not None:
            doc["sample_duration"] = self.sample_duration
        if self.pressure is not None:
            doc["pressure"] = self.pressure
        if self.wind_speed is not None:
            doc["wind_speed"] = self.wind_speed
        if self.wind_direction is not None:
            doc["wind_direction"] = self.wind_direction
        if self.photo_filename is not None:
            # Open and encode the image file
            with open(self.photo_filename, "rb") as image_file:
                image_data = Binary(image_file.read())  # Binary is Mongo-safe
                doc["photo"] = image_data

        return doc
            

class WeatherReport:
    def __init__(self, 
                 date,
                 location,
                 last_modified,
                 version = 1,                 
                 station = None, 
                 owner = None,
                 readings = None, 
                 observations = None,
                 day_summary = None):
        self.date = date
        self.location = location
        self.last_modified = last_modified
        self.version = version
        self.station = station
        self.owner = owner
        self.readings = readings
        self.observations = observations
        self.day_summary = day_summary

    # create a document in a suitable format for the weather_reports collection
    def get_weather_report_doc(self):
        doc = {"date": self.date,
               "version": self.version,
               "last_modified": self.last_modified,
               "location": self.location}
        
        if self.station is not None:
            doc["station"] = self.station.get_subset_for_weather_report()
        if self.owner is not None:
            doc["owner"] = self.owner.get_subset_for_weather_report()
        if self.readings is not None:
            doc["readings"] = [r.get_doc_for_weather_report() for r in self.readings]
        if self.observations is not None:
            doc["observations"] = [ob.get_doc_for_weather_report() for ob in self.observations]
        if self.day_summary is not None:
            doc["day_summary"] = self.day_summary            
        return doc
    
class WeatherStation:
    def __init__(self, 
                 station_id,
                 name, 
                 location,
                 owner,
                 status, 
                 latest_maintenance = None):
        self.station_id = station_id
        self.name = name
        self.location = location
        self.owner = owner
        self.status = status
        self.latest_maintenance = latest_maintenance

    # create a document in a suitable format for the weather_stations collection
    def get_weather_station_doc(self):
        doc = {"_id": self.station_id,
               "name": self.name,
               "location": self.location,
               "owner": self.owner.get_subset_for_weather_station(),
               "status": self.status}
        
        if self.latest_maintenance is not None:
            doc["latest_maintenance"] = self.latest_maintenance.get_subset_for_weather_station()

        return doc    
    
    # create a document containing a subset of the data in a suitable format for embedding in a weather report
    def get_subset_for_weather_report(self):
        doc = {"station_id": self.station_id,
               "name": self.name}
        return doc

# An Institution is one of the types of owner of a Weather Station 
# - (government/university/commerical etc)
# - contact details are considered public and therefore can be embedded without encryption
class Institution:
    def __init__(self, 
                 user_id,
                 password,
                 institution_name,
                 institution_type,
                 contact_name,
                 contact_email,
                 contact_telephone):
        self.user_id = user_id
        self.password = password
        self.institution_name = institution_name
        self.institution_type = institution_type
        self.contact_name = contact_name
        self.contact_email = contact_email
        self.contact_telephone = contact_telephone

    # create a document containing all the data in a suitable format for the users collection
    # - the password is encrypted before storage
    def get_user_doc(self):
        doc = {"_id": self.user_id,
               "password": EncryptionHelper.hash_password(self.password),
               "user_type": self.institution_type,
               "institution": self.institution_name,
               "contact": self.contact_name,
               "email": self.contact_email,
               "telephone": self.contact_telephone}
        return doc    
    
    # create a document containing a subset of the data in a suitable format for embedding in a weather_station
    def get_subset_for_weather_station(self):
        doc = {"owner_type": self.institution_type,
               "user_id": self.user_id,
               "name": self.institution_name,                               
               "contact": self.contact_name,
               "email": self.contact_email,
               "telephone": self.contact_telephone}
        return doc
    
    # create a document containing a subset of the data in a suitable format for embedding in a weather report
    def get_subset_for_weather_report(self):
        doc = {"owner_type": self.institution_type,
               "user_id": self.user_id,
               "name": self.institution_name}
        return doc

    
# A private individual that may own a private Weather Station,
# They may contribute manual Weather Observations, or just be registered to use the weather data
# - only the user_id and display_name are considered to be public
# - all other contact information is personal and are encrypted in the database
# - uses Bcrypt to store the hash of the password rather than the password itself 
class WeatherWatcher:
    def __init__(self, 
                 user_id,
                 password,
                 display_name,
                 name,
                 email):
        self.user_id = user_id
        self.password = password
        self.display_name = display_name
        self.name = name
        self.email = email


    # create a document containing all the data in a suitable format for the users collection
    # - the password is hashed before storage
    def get_user_doc(self):
        doc = {"_id": self.user_id,
               "password": EncryptionHelper.hash_password(self.password),
               "user_type": "Private",
               "name": EncryptionHelper.encrypt(self.name),
               "display_name": self.display_name,
               "email": EncryptionHelper.encrypt(self.email),
               }
        return doc    
    
    # create a subset of the data for embedding in a weather_station
    def get_subset_for_weather_station(self):
        doc = {"owner_type": "Private",
               "user_id": self.user_id,
               "name": self.display_name}
        return doc
    
    # create a subset of the data for embedding in a weather_report
    def get_subset_for_weather_report(self):
        doc = {"owner_type": "Private",
               "user_id": self.user_id,
               "name": self.display_name}
        return doc
    
# A database administrator that will be allowed read/write access to all collections
# - uses Bcrypt to store the hash of the password rather than the password itself 
class Administrator:
    def __init__(self, 
                 user_id,
                 password,
                 name,
                 email):
        self.user_id = user_id
        self.password = password
        self.name = name
        self.email = email


    # create a document containing all the data in a suitable format for the users collection
    # - the password is encrypted before storage
    def get_user_doc(self):
        doc = {"_id": self.user_id,
               "password": EncryptionHelper.hash_password(self.password),
               "user_type": "Admin",
               "name": EncryptionHelper.encrypt(self.name),
               "email": EncryptionHelper.encrypt(self.email),
               }
        return doc      
   
class Technician:
    def __init__(self, 
                 tech_id,
                 name,
                 company,
                 email,
                 telephone):
        self.tech_id = tech_id
        self.name = name
        self.company = company
        self.email = email
        self.telephone = telephone

    # create a document containing the data in a suitable format for the technicians collection
    def get_technician_doc(self):
        doc = {"_id": self.tech_id,
               "name": self.name,
               "company": self.company,
               "email": self.email,
               "telephone": self.telephone}               
        return doc     

class MaintenanceLogItem:
    def __init__(self, 
                 timestamp,
                 station_id,
                 tech_id,
                 report):
        self.timestamp = timestamp
        self.station_id = station_id
        self.tech_id = tech_id
        self.report = report

    # create a document containing the data in a suitable format for the maintenance_logs collection
    def get_maintenance_log_doc(self):
        doc = {"timestamp": self.timestamp,
               "station_id": self.station_id,
               "tech_id": self.tech_id,
               "report": self.report}               
        return doc            
    
    # take a subset of the data for embedding in the Weather Station document
    def get_subset_for_weather_station(self):
        doc = {"timestamp": self.timestamp,
               "tech_id": self.tech_id,
               "report": self.report}               
        return doc         
