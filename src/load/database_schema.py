from sqlalchemy import (
    create_engine, Column, Integer, String, Float, 
    DateTime, Boolean, BigInteger, Text, Index
)
from sqlalchemy.orm import declarative_base, sessionmaker  # Updated import
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Base = declarative_base()  # This is the new way in SQLAlchemy 2.0

class WeatherFact(Base):
    """
    Fact table: Main weather measurements
    """
    __tablename__ = 'weather_fact'
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign keys / identifiers
    city_id = Column(BigInteger, nullable=False, index=True)
    city_name = Column(String(100), nullable=False, index=True)
    country_code = Column(String(10), index=True)
    
    # Geographic coordinates
    longitude = Column(Float)
    latitude = Column(Float)
    
    # Weather condition
    weather_id = Column(Integer)
    weather_main = Column(String(50))
    weather_description = Column(String(200))
    weather_icon = Column(String(10))
    
    # Temperature measurements (Celsius)
    temp_celsius = Column(Float)
    feels_like_celsius = Column(Float)
    temp_min_celsius = Column(Float)
    temp_max_celsius = Column(Float)
    temp_category = Column(String(20))
    
    # Atmospheric measurements
    pressure_hpa = Column(Float)
    humidity_percent = Column(Float)
    humidity_category = Column(String(20))
    sea_level_hpa = Column(Float)
    ground_level_hpa = Column(Float)
    
    # Wind measurements
    wind_speed_mps = Column(Float)
    wind_direction_deg = Column(Float)
    wind_gust_mps = Column(Float)
    wind_strength = Column(String(20))
    
    # Cloud and visibility
    cloudiness_percent = Column(Float)
    visibility_meters = Column(Float)
    
    # Precipitation
    rain_1h_mm = Column(Float, default=0)
    rain_3h_mm = Column(Float, default=0)
    snow_1h_mm = Column(Float, default=0)
    snow_3h_mm = Column(Float, default=0)
    is_raining = Column(Boolean, default=False)
    is_snowing = Column(Boolean, default=False)
    
    # Timestamps (UTC)
    dt_unix = Column(BigInteger)
    dt_datetime = Column(DateTime, index=True)
    timezone_offset_sec = Column(Integer)
    sunrise_unix = Column(BigInteger)
    sunset_unix = Column(BigInteger)
    sunrise_datetime = Column(DateTime)
    sunset_datetime = Column(DateTime)
    
    # Derived metrics
    daylight_hours = Column(Float)
    heat_index_diff = Column(Float)
    weather_severity = Column(Float)
    
    # Time dimensions
    date = Column(String(20), index=True)
    hour = Column(Integer)
    day_of_week = Column(String(20))
    month = Column(String(20))
    
    # Metadata
    extracted_at = Column(String(50))
    extracted_at_datetime = Column(DateTime)
    extraction_source = Column(String(100))
    loaded_at = Column(DateTime, nullable=False, index=True)
    
    # Indexes for common queries
    __table_args__ = (
        Index('idx_city_date', 'city_name', 'date'),
        Index('idx_country_date', 'country_code', 'date'),
        Index('idx_datetime', 'dt_datetime'),
    )

class CityDimension(Base):
    """
    Dimension table: City master data
    """
    __tablename__ = 'city_dimension'
    
    city_id = Column(BigInteger, primary_key=True)
    city_name = Column(String(100), nullable=False, unique=True)
    country_code = Column(String(10))
    longitude = Column(Float)
    latitude = Column(Float)
    timezone_offset_sec = Column(Integer)
    first_seen = Column(DateTime)
    last_updated = Column(DateTime)

class LoadHistory(Base):
    """
    Audit table: Track all data loads
    """
    __tablename__ = 'load_history'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    load_timestamp = Column(DateTime, nullable=False)
    records_loaded = Column(Integer)
    source_file = Column(String(500))
    status = Column(String(20))  # 'success', 'failed', 'partial'
    error_message = Column(Text)
    duration_seconds = Column(Float)


def create_database_schema(engine):
    """
    Create all tables in the database
    """
    logger.info("Creating database schema...")
    
    try:
        Base.metadata.create_all(engine)
        logger.info(" Schema created successfully!")
        
        # Print table information
        logger.info(f"\n Tables created:")
        for table in Base.metadata.tables.keys():
            logger.info(f"   • {table}")
        
        return True
    except Exception as e:
        logger.error(f" Error creating schema: {e}")
        return False


def get_database_engine(connection_string=None):
    """
    Create SQLAlchemy engine
    """
    if connection_string is None:
        connection_string = (
            "postgresql://weather_user:weather_pass123@localhost:5432/weather_analytics"
        )
    
    engine = create_engine(
        connection_string,
        echo=False,  # Set to True for SQL query logging
        pool_pre_ping=True  # Verify connections before using
    )
    
    return engine


if __name__ == "__main__":
    print("=" * 70)
    print("CREATING DATABASE SCHEMA")
    print("=" * 70)
    
    engine = get_database_engine()
    success = create_database_schema(engine)
    
    if success:
        print("\n Database schema is ready!")
        print("\n Tables created:")
        print("   • weather_fact (main data)")
        print("   • city_dimension (city master)")
        print("   • load_history (audit log)")
    else:
        print("\n Failed to create schema!")