# Weather Data Management System

A comprehensive MongoDB-based weather data management system built with Python, designed to handle multi-source meteorological data ingestion, storage, and analysis for weather stations, radiosonde launches, and manual weather observations.

## Overview

This advanced NoSQL database system addresses real-world challenges in meteorological data management by providing a scalable, high-performance solution for handling diverse weather data sources. The system integrates automated weather station readings, radiosonde telemetry, and manual weather observations into a unified platform supporting geospatial analytics, time-series processing, and multi-tenant security.

## Features

### Core Functionality
- **Multi-Source Data Integration**: Automated ingestion from CSV (Open-Meteo), GeoJSON (Windy.com), and manual observations with photos
- **Geospatial Analytics**: Find weather stations within radius, location-based temperature analysis, and geographic weather pattern detection
- **Time-Series Processing**: High-frequency sensor data with efficient querying, aggregation pipelines, and temporal analysis
- **Weather Balloon Tracking**: Complete radiosonde data processing with 3D altitude-based atmospheric profiling
- **Maintenance Management**: Comprehensive technician activity tracking and automated maintenance scheduling

### Advanced Features
- **Multi-Tenant Security**: Role-based access control for institutions, private users, administrators with encrypted PII protection
- **Complex Analytics**: Weather extremes detection, temperature trend analysis, storage optimization reporting, and predictive insights
- **Real-Time Monitoring**: Weather station status tracking, alert systems, and operational health dashboards  
- **Comprehensive Indexing**: Optimized geospatial (2dsphere), temporal, and compound indexes for sub-second query performance
- **Data Encryption**: bcrypt password hashing and Fernet symmetric encryption for sensitive data protection

### User Roles
- **Institutions**: Government agencies, airports, universities with public contact information
- **Weather Watchers**: Private individuals with encrypted personal data and manual observation capabilities
- **Technicians**: Maintenance personnel with activity logging and station assignment tracking
- **Administrators**: Full system access with audit trail and user management capabilities

## Technology Stack

- **Database**: MongoDB 4.4+ with advanced aggregation framework
- **Backend**: Python 3.8+, PyMongo driver
- **Security**: bcrypt (password hashing), Cryptography/Fernet (data encryption)
- **Data Processing**: Pandas-like operations with MongoDB aggregation pipelines
- **Geospatial**: MongoDB GeoJSON support with 2dsphere indexing
- **Logging**: Custom dual-output logging system (console + file)
- **Testing**: Comprehensive CRUD and read-only query test suites

## Quick Start

### Prerequisites
- Python 3.8 or higher
- MongoDB 4.4 or higher (running locally or remote)
- 4GB RAM minimum (for large dataset processing)
- pip (Python package manager)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/weather-data-management.git
   cd weather-data-management
   ```

2. **Install dependencies**
   ```bash
   pip install pymongo bcrypt cryptography
   ```

3. **Start MongoDB service**
   ```bash
   # On Windows (if installed as service)
   net start MongoDB
   
   # On macOS with Homebrew
   brew services start mongodb-community
   
   # Manual start
   mongod --dbpath /path/to/your/db --port 27017
   ```

4. **Initialize database and import data**
   ```bash
   python build_database_and_import_data.py
   ```

   This comprehensive setup process will:
   - Create the `weather` database with all collections
   - Implement optimized indexing strategy (geospatial, temporal, compound)
   - Import weather data from 17 UK weather stations (10,000+ readings)
   - Generate realistic synthetic maintenance logs and user accounts
   - Import radiosonde data from multiple ground stations
   - Create sample manual observations with photo attachments

5. **Verify installation**
   ```bash
   # Test read operations (non-destructive)
   python test_read_queries.py
   
   # Test CRUD operations (modifies database)
   python test_crud.py
   ```

6. **Access the system**
   - MongoDB Compass: `mongodb://localhost:27017`
   - Database name: `weather`
   - Check logs: `build_database_and_import_data.log`, `test_CRUD.log`, `test_read_queries.log`

## Project Structure

```
weather-data-management/
├── build_database_and_import_data.py    # Main database initialization and data import
├── model.py                             # Domain models, business logic, and data validation
├── helper.py                            # MongoDB operations, logging utilities, date helpers
├── encrypt.py                           # Security utilities (bcrypt, Fernet encryption)
├── test_crud.py                         # CRUD operations testing and scenarios
├── test_read_queries.py                 # Read query performance and validation testing
├── data/                                # Sample datasets
│   ├── OpenMeteo/                       # CSV files from Open-Meteo weather API
│   │   ├── open-meteo-London.csv        # Hourly readings for major UK cities
│   │   ├── open-meteo-Manchester.csv
│   │   ├── open-meteo-Birmingham.csv
│   │   └── ... (17 weather stations total)
│   ├── Windy/                          # GeoJSON radiosonde data from Windy.com
│   │   ├── 03743_20250401_080000.geojson   # Larkhill ground station launches
│   │   ├── 03354_20250404_230000.geojson   # Nottingham ground station launches
│   │   └── ... (9 balloon launches total)
│   └── images/                         # Sample weather observation photographs
│       ├── flood.jpg                   # Heavy rain flood documentation
│       ├── double_rainbow.jpg          # Light shower weather phenomenon
│       └── morning_frost.jpg           # Sunny intervals frost conditions
├── logs/                               # Generated log files
├── requirements.txt                    # Python dependencies
└── README.md                          # This comprehensive documentation
```

## Database Architecture

### Collections Overview

| Collection | Purpose | Documents | Key Features |
|------------|---------|-----------|--------------|
| `weather_reports` | Daily weather data with embedded hourly readings | ~6,000+ | Geospatial indexing, temporal queries |
| `weather_stations` | Station metadata, ownership, operational status | 17 | Location data, maintenance tracking |
| `weather_balloon_reports` | Radiosonde telemetry with 3D atmospheric data | 9 | Altitude-based indexing, launch tracking |
| `users` | Multi-tenant user management with role-based access | 8 | Encrypted PII, institutional contacts |
| `technicians` | Maintenance personnel and company information | 8 | Activity correlation, skills tracking |
| `maintenance_logs` | Comprehensive audit trail for all station activities | 500+ | Timestamped logs, technician assignments |

### Advanced Document Design

#### Weather Report (Embedded Time-Series)
```json
{
  "_id": ObjectId("..."),
  "date": ISODate("2025-03-01T00:00:00Z"),
  "version": 1,
  "last_modified": ISODate("2025-03-01T23:59:59Z"),
  "location": {
    "type": "Point",
    "coordinates": [-0.1630249, 51.493847]
  },
  "station": {
    "station_id": "WS-LON002", 
    "name": "London"
  },
  "owner": {
    "owner_type": "Government",
    "user_id": "metoff1",
    "name": "Met Office"
  },
  "readings": [
    {
      "timestamp": ISODate("2025-03-01T00:00:00Z"),
      "sample_duration": 3600,
      "temp": 6.5,
      "dewpoint": 2.5,
      "humidity": 76,
      "pressure": 1032.7,
      "precip": 0.0,
      "cloud_cover": 25,
      "wind_speed": 2.62,
      "wind_direction": 43,
      "sunshine": 0,
      "soil": {
        "0_to_7cm": {"temp": 9.6, "moisture": 0.012},
        "7_to_28cm": {"temp": 11.7, "moisture": 0.126},
        "28_to_100cm": {"temp": 10.3, "moisture": 0.28},
        "100_to_255cm": {"temp": 8.0, "moisture": 0.291}
      }
    }
  ],
  "day_summary": {
    "temp_mean": 8.2,
    "temp_min": 6.5,
    "temp_max": 12.1,
    "dewpoint_mean": 4.8,
    "humidity_mean": 73,
    "humidity_min": 65,
    "humidity_max": 82,
    "pressure_mean": 1031.5,
    "pressure_min": 1029.2,
    "pressure_max": 1033.8,
    "precip_sum": 2.3,
    "cloud_cover_mean": 45,
    "wind_speed_mean": 3.1,
    "wind_speed_min": 1.8,
    "wind_speed_max": 5.2,
    "sunshine": 18000
  }
}
```

#### Weather Balloon Report (3D Atmospheric Data)
```json
{
  "_id": ObjectId("..."),
  "launch_date": ISODate("2025-04-01T08:00:00Z"),
  "station": {
    "station_id": "03743",
    "name": "Larkhill",
    "owner": {
      "owner_type": "Government",
      "user_id": "metoff1",
      "name": "Met Office"
    }
  },
  "location": {
    "type": "Point",
    "coordinates": [-1.8094, 51.1942, 142]
  },
  "radiosonde": {
    "serial": "P2330465",
    "software": "SRS-C50 v2.15.5"
  },
  "readings": [
    {
      "timestamp": ISODate("2025-04-01T08:02:15Z"),
      "location": {
        "type": "Point",
        "coordinates": [-1.8095, 51.1943, 445]
      },
      "gpheight": 445,
      "temp": 15.8,
      "dewpoint": 12.3,
      "pressure": 965.2,
      "wind_speed": 4.2,
      "wind_direction": 285.7
    }
  ]
}
```

## Advanced Configuration

### MongoDB Connection Settings
```python
# Default development configuration
client = MongoClient("mongodb://localhost:27017/")
db_name = 'weather'

# Production configuration with authentication
client = MongoClient(
    "mongodb://username:password@prod-server:27017/",
    authSource='admin',
    ssl=True,
    replicaSet='weather-rs'
)
```

### Performance Optimization Configuration
```python
# Index strategy for optimal query performance
collection.create_index([("location", GEOSPHERE)])                    # Geospatial queries
collection.create_index([("date", DESCENDING)])                       # Recent data access
collection.create_index([("readings.timestamp", DESCENDING)])         # Time-series queries
collection.create_index([("station.station_id", ASCENDING)])          # Station filtering
collection.create_index([("owner.owner_type", ASCENDING)])            # Multi-tenant queries

# Compound indexes for complex queries
collection.create_index([("station.station_id", ASCENDING), ("date", DESCENDING)])
collection.create_index([("owner.user_id", ASCENDING), ("last_modified", DESCENDING)])
```

### Security Configuration
```python
# Password hashing configuration
BCRYPT_ROUNDS = 12  # Production-grade security

# Data encryption key management
# WARNING: Use proper key management in production
def get_encryption_key():
    return os.environ.get('WEATHER_DB_KEY', 'fallback_insecure_key')
```

## API Operations & Query Examples

### Multi-Source Data Import Operations
```python
# Weather station CSV data import
importer = OpenMeteoDataImporter(mongo_handler)
importer.import_data(
    station_id="WS-LON002", 
    station_name="London",
    filename="data/OpenMeteo/open-meteo-London.csv",
    owner=met_office_institution,
    technicians=[technician_list]
)

# Radiosonde GeoJSON data import
radiosonde_importer = RadiosondeDataImporter(mongo_handler)
radiosonde_importer.import_data(
    launch_date=datetime(2025, 4, 1, 8, 0),
    ground_station_id="03743",
    ground_station_name="Larkhill",
    filename="data/Windy/03743_20250401_080000.geojson",
    owner=met_office_institution
)
```

### Advanced Geospatial Queries
```python
# Find weather stations within radius using spherical geometry
def find_stations_near_location(db, longitude, latitude, max_distance_miles=50):
    earth_radius_miles = 3963.2
    radius_radians = max_distance_miles / earth_radius_miles
    
    pipeline = [{
        "$geoNear": {
            "near": {"type": "Point", "coordinates": [longitude, latitude]},
            "distanceField": "distance_miles",
            "maxDistance": radius_radians,
            "spherical": True,
            "distanceMultiplier": earth_radius_miles
        }
    }]
    
    return db.weather_stations.aggregate(pipeline)

# Multi-station temperature analysis with geographic filtering
def calculate_regional_temperature_trends(db, center_coords, radius_miles, date_range):
    pipeline = [
        {
            "$geoNear": {
                "near": {"type": "Point", "coordinates": center_coords},
                "distanceField": "distance",
                "maxDistance": radius_miles * 1609.34,  # Convert to meters
                "spherical": True
            }
        },
        {"$match": {"date": {"$gte": date_range[0], "$lte": date_range[1]}}},
        {"$unwind": "$readings"},
        {
            "$group": {
                "_id": {
                    "station": "$station.station_id",
                    "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date"}}
                },
                "avg_temp": {"$avg": "$readings.temp"},
                "station_name": {"$first": "$station.name"},
                "distance": {"$first": "$distance"}
            }
        },
        {"$sort": {"distance": 1, "_id.date": -1}}
    ]
    
    return db.weather_reports.aggregate(pipeline)
```

### Complex Time-Series Analytics
```python
# Weather extremes detection with business logic
def detect_weather_extremes(db, station_id):
    pipeline = [
        {"$match": {"station.station_id": station_id}},
        {"$addFields": {
            "extreme_conditions": {
                "$switch": {
                    "branches": [
                        {
                            "case": {"$and": [
                                {"$gt": ["$day_summary.wind_speed_max", 12]},
                                {"$gt": ["$day_summary.precip_sum", 15]}
                            ]},
                            "then": "Storm"
                        },
                        {
                            "case": {"$lt": ["$day_summary.temp_min", -5]},
                            "then": "Severe Cold"
                        },
                        {
                            "case": {"$gt": ["$day_summary.temp_max", 30]},
                            "then": "Extreme Heat"
                        }
                    ],
                    "default": "Normal"
                }
            }
        }},
        {"$match": {"extreme_conditions": {"$ne": "Normal"}}},
        {"$project": {
            "date": 1,
            "extreme_conditions": 1,
            "temp_range": {
                "$concat": [
                    {"$toString": "$day_summary.temp_min"}, 
                    "°C to ", 
                    {"$toString": "$day_summary.temp_max"}, 
                    "°C"
                ]
            },
            "wind_max": "$day_summary.wind_speed_max",
            "precipitation": "$day_summary.precip_sum"
        }},
        {"$sort": {"date": -1}}
    ]
    
    return db.weather_reports.aggregate(pipeline)

# Temperature trend analysis with arithmetic operations
def analyze_temperature_anomalies(db, station_id, year):
    pipeline = [
        {"$match": {
            "station.station_id": station_id,
            "$expr": {"$eq": [{"$year": "$date"}, year]}
        }},
        {"$unwind": "$readings"},
        {"$addFields": {
            "hour": {"$hour": "$readings.timestamp"},
            "temp_fahrenheit": {
                "$round": [
                    {"$add": [{"$multiply": ["$readings.temp", 1.8]}, 32]}, 
                    1
                ]
            }
        }},
        {"$match": {"hour": {"$in": [9, 15]}}},  # 9 AM and 3 PM readings
        {"$group": {
            "_id": "$date",
            "temp_9am": {"$max": {"$cond": [{"$eq": ["$hour", 9]}, "$readings.temp", null]}},
            "temp_3pm": {"$max": {"$cond": [{"$eq": ["$hour", 15]}, "$readings.temp", null]}},
            "temp_9am_f": {"$max": {"$cond": [{"$eq": ["$hour", 9]}, "$temp_fahrenheit", null]}},
            "temp_3pm_f": {"$max": {"$cond": [{"$eq": ["$hour", 15]}, "$temp_fahrenheit", null]}}
        }},
        {"$addFields": {
            "temperature_drop": {"$subtract": ["$temp_9am", "$temp_3pm"]},
            "anomaly_type": {
                "$cond": [
                    {"$gt": ["$temp_9am", "$temp_3pm"]},
                    "Cooler Afternoon",
                    "Normal Warming"
                ]
            }
        }},
        {"$match": {"anomaly_type": "Cooler Afternoon"}},
        {"$sort": {"temperature_drop": -1}}
    ]
    
    return db.weather_reports.aggregate(pipeline)
```

### User Authentication & Security Operations
```python
def authenticate_user(db, user_id, password):
    """Secure user authentication with encrypted data handling"""
    user = db.users.find_one({"_id": user_id})
    
    if not user:
        return {"authenticated": False, "error": "User not found"}
    
    # Verify password using bcrypt
    if not EncryptionHelper.verify_password(password, user.get("password", "")):
        return {"authenticated": False, "error": "Invalid password"}
    
    # Decrypt sensitive information for private users
    user_info = {
        "authenticated": True,
        "user_id": user_id,
        "user_type": user.get("user_type"),
        "display_name": user.get("display_name", user.get("institution"))
    }
    
    if user.get("user_type") in ["Private", "Admin"]:
        user_info.update({
            "full_name": EncryptionHelper.decrypt(user.get("name")),
            "email": EncryptionHelper.decrypt(user.get("email"))
        })
    
    return user_info
```

## Comprehensive Testing

### Test Suites Overview
- **Authentication Testing**: Multi-user type login validation with encryption
- **Geospatial Testing**: Location-based queries and distance calculations  
- **Time-Series Testing**: Temporal aggregations and trend analysis
- **CRUD Testing**: Complete lifecycle operations with versioning
- **Security Testing**: Data encryption/decryption and role-based access
- **Performance Testing**: Index effectiveness and query optimization

### Key Test Scenarios
```python
# Multi-tenant authentication testing
validate_user(db, "ps2004", "Pa$$1234")      # Private weather watcher
validate_user(db, "metoff1", "Pa$$5678")     # Government institution
validate_user(db, "admin1", "Pa$$4422")      # System administrator

# Geospatial analysis testing
find_stations_near_location(db, 1.1481, 52.0597, 70)  # 70 miles from Ipswich
calculate_average_temperature_area(
    db, -0.163, 51.494, 100, 11, 14,
    datetime(2024, 8, 1), datetime(2024, 9, 1)
)

# Weather balloon data analysis
get_weather_balloon_readings_by_page(db, "03743", datetime(2025, 4, 1), 1, 5)

# Advanced analytics testing  
create_cooler_afternoons_report("WS-SOU001", 2024)
get_airport_wind_speeds(db, datetime(2024, 6, 21), datetime(2024, 9, 21))
```

### Performance Benchmarks
- **Geospatial queries**: <100ms for 50-mile radius searches
- **Time-series aggregations**: <200ms for monthly temperature averages
- **Complex analytics**: <500ms for weather extremes detection
- **User authentication**: <50ms including decryption operations

## Enterprise Security Features

### Multi-Layer Data Protection
```python
class SecurityArchitecture:
    """Comprehensive security implementation"""
    
    @staticmethod
    def hash_password(password: str) -> str:
        """Production-grade password hashing with bcrypt"""
        return bcrypt.hashpw(
            password.encode("utf-8"),
            bcrypt.gensalt(rounds=12)
        ).decode("utf-8")
    
    @staticmethod  
    def encrypt_pii(sensitive_data: str) -> str:
        """Encrypt personally identifiable information"""
        fernet = Fernet(SecurityHelper.get_encryption_key())
        return fernet.encrypt(sensitive_data.encode()).decode()
    
    @staticmethod
    def role_based_access(user_type: str, operation: str) -> bool:
        """Implement role-based access control"""
        permissions = {
            "Admin": ["read", "write", "delete", "configure"],
            "Government": ["read", "write", "maintain"],
            "Airport": ["read", "write"],
            "University": ["read", "write"],
            "Private": ["read", "observe"]
        }
        return operation in permissions.get(user_type, [])
```

### Data Privacy Implementation
- **Selective Encryption**: Only PII data encrypted, public institutional data remains searchable
- **Access Logging**: All database operations logged with user attribution
- **Data Anonymization**: Personal identifiers removed from analytical queries
- **Secure Key Management**: Environment-based encryption key storage

## Production Deployment Guide

### MongoDB Production Configuration
```python
# Production connection with replica set
client = MongoClient(
    "mongodb://weather_user:secure_password@mongo-rs-0:27017,mongo-rs-1:27017,mongo-rs-2:27017/weather?replicaSet=weather-rs",
    maxPoolSize=50,
    wtimeout=5000,
    ssl=True,
    ssl_cert_reqs='CERT_REQUIRED',
    ssl_ca_certs='/path/to/ca.pem'
)

# Sharding configuration for large datasets
db.runCommand({
    "shardCollection": "weather.weather_reports",
    "key": {"station.station_id": 1, "date": 1}
})
```

### Environment Configuration
```bash
# Production environment variables
export WEATHER_DB_HOST=mongodb-cluster.example.com
export WEATHER_DB_PORT=27017
export WEATHER_DB_NAME=weather_production
export WEATHER_DB_USER=weather_app
export WEATHER_DB_PASSWORD=secure_production_password
export WEATHER_ENCRYPTION_KEY=base64_encoded_32_byte_key
export WEATHER_LOG_LEVEL=INFO
export WEATHER_BACKUP_ENABLED=true
```

### Docker Production Setup
```dockerfile
# Production Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user
RUN useradd --create-home --shell /bin/bash weather
RUN chown -R weather:weather /app
USER weather

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD python -c "from pymongo import MongoClient; MongoClient().admin.command('ismaster')"

EXPOSE 8000

CMD ["python", "weather_api_server.py"]
```

### Monitoring & Alerting
```python
# Production monitoring setup
import logging
from pymongo.monitoring import CommandListener

class WeatherDBMonitor(CommandListener):
    def started(self, event):
        if event.command.get('find') or event.command.get('aggregate'):
            logging.info(f"Query started: {event.command_name} on {event.database_name}")
    
    def succeeded(self, event):
        if event.duration_micros > 1000000:  # Log slow queries (>1s)
            logging.warning(f"Slow query detected: {event.duration_micros/1000}ms")
    
    def failed(self, event):
        logging.error(f"Query failed: {event.failure}")

# Register monitoring
client.add_listener(WeatherDBMonitor())
```

## Performance Optimization Guide

### Query Optimization Strategies
```python
# Optimized geospatial query with projection
def optimized_station_search(db, coordinates, radius_km):
    return db.weather_stations.aggregate([
        {
            "$geoNear": {
                "near": {"type": "Point", "coordinates": coordinates},
                "distanceField": "distance",
                "maxDistance": radius_km * 1000,
                "spherical": True,
                "limit": 20
            }
        },
        {
            "$project": {
                "name": 1,
                "location": 1,
                "owner.name": 1,
                "distance": 1
            }
        }
    ])

# Efficient time-series aggregation with date optimization
def optimized_temperature_analysis(db, station_id, start_date, end_date):
    return db.weather_reports.aggregate([
        {
            "$match": {
                "station.station_id": station_id,
                "date": {"$gte": start_date, "$lte": end_date}
            }
        },
        {"$unwind": "$readings"},
        {
            "$group": {
                "_id": {
                    "$dateToString": {"format": "%Y-%m-%d", "date": "$readings.timestamp"}
                },
                "avg_temp": {"$avg": "$readings.temp"},
                "min_temp": {"$min": "$readings.temp"},
                "max_temp": {"$max": "$readings.temp"},
                "sample_count": {"$sum": 1}
            }
        },
        {"$sort": {"_id": 1}},
        {"$limit": 1000}  # Prevent excessive memory usage
    ])
```

### Scaling Recommendations
- **Read Replicas**: Deploy read-only replicas for analytical workloads
- **Connection Pooling**: Configure optimal connection pool sizes (50-100)  
- **Index Monitoring**: Regular index usage analysis and optimization
- **Data Archival**: Archive historical data older than 2 years to cold storage
- **Caching Layer**: Implement Redis for frequently accessed aggregated data

## Contributing

### Development Environment Setup
```bash
# Development setup
git clone https://github.com/yourusername/weather-data-management.git
cd weather-data-management

# Create development environment
python -m venv weather_dev_env
source weather_dev_env/bin/activate  # On Windows: weather_dev_env\Scripts\activate

# Install development dependencies
pip install pymongo bcrypt cryptography pytest black flake8

# Set up pre-commit hooks
pip install pre-commit
pre-commit install
```

### Code Quality Standards
- **PEP 8 Compliance**: Use `black` for automatic code formatting
- **Type Hints**: Include type annotations for all function parameters and returns
- **Docstring Standards**: Google-style docstrings for all classes and methods
- **Error Handling**: Comprehensive exception handling with informative error messages
- **Testing Requirements**: 90%+ test coverage for new features

### Pull Request Process
1. Fork repository and create feature branch
2. Implement changes with comprehensive tests
3. Run full test suite: `python -m pytest tests/`
4. Update documentation and README as needed
5. Submit PR with detailed description and test results

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Troubleshooting

### Common Issues & Solutions

#### MongoDB Connection Issues
```bash
# Check MongoDB service status
systemctl status mongod  # Linux
brew services list | grep mongodb  # macOS

# Test connection manually
python -c "from pymongo import MongoClient; print(MongoClient().admin.command('ismaster'))"
```

#### Performance Issues
```bash
# Check index usage
db.weather_reports.aggregate([{"$indexStats": {}}])

# Monitor slow queries
db.setProfilingLevel(2, {slowms: 1000})
db.system.profile.find().pretty()
```

#### Memory Issues During Data Import
```python
# Process data in smaller batches
def batch_import(data_list, batch_size=1000):
    for i in range(0, len(data_list), batch_size):
        batch = data_list[i:i + batch_size]
        collection.insert_many(batch)
        print(f"Processed {min(i + batch_size, len(data_list))}/{len(data_list)} documents")
```

#### Data Validation Errors
```python
# Enable detailed validation logging
logging.basicConfig(level=logging.DEBUG)

# Check data format before import
def validate_weather_data(data):
    required_fields = ['timestamp', 'temp', 'humidity', 'pressure']
    for field in required_fields:
        if field not in data or data[field] is None:
            raise ValueError(f"Missing required field: {field}")
```

