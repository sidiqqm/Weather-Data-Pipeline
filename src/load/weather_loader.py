import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from datetime import datetime
from glob import glob
import os
import logging

from src.load.database_schema import (
    get_database_engine, 
    WeatherFact, 
    CityDimension, 
    LoadHistory
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/loading.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class WeatherLoader:
    """
    Load transformed weather data into PostgreSQL
    """
    
    def __init__(self, connection_string=None):
        self.engine = get_database_engine(connection_string)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        logger.info("WeatherLoader initialized")
    
    def load_processed_data(self, filepath=None):
        """
        Load latest processed CSV file
        """
        if filepath is None:
            # Find latest processed file
            csv_files = glob('data/processed/weather_clean_*.csv')
            if not csv_files:
                logger.error(" No processed CSV files found!")
                return None
            filepath = max(csv_files, key=os.path.getctime)
        
        logger.info(f"Loading data from: {filepath}")
        
        try:
            df = pd.read_csv(filepath)
            
            # Convert datetime columns
            datetime_cols = ['dt_datetime', 'sunrise_datetime', 'sunset_datetime', 'extracted_at_datetime']
            for col in datetime_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
            
            logger.info(f" Loaded {len(df)} records from CSV")
            return df, filepath
        
        except Exception as e:
            logger.error(f" Error loading CSV: {e}")
            return None, None
    
    def upsert_city_dimension(self, df):
        """
        Update or insert city master data
        """
        logger.info("Upserting city dimension...")
        
        city_data = df[[
            'city_id', 'city_name', 'country_code', 
            'longitude', 'latitude', 'timezone_offset_sec'
        ]].drop_duplicates(subset=['city_id'])
        
        upserted = 0
        
        for _, row in city_data.iterrows():
            city = self.session.query(CityDimension).filter_by(
                city_id=row['city_id']
            ).first()
            
            if city:
                # Update existing
                city.city_name = row['city_name']
                city.country_code = row['country_code']
                city.longitude = row['longitude']
                city.latitude = row['latitude']
                city.timezone_offset_sec = row['timezone_offset_sec']
                city.last_updated = datetime.now()
            else:
                # Insert new
                city = CityDimension(
                    city_id=int(row['city_id']),
                    city_name=row['city_name'],
                    country_code=row['country_code'],
                    longitude=float(row['longitude']),
                    latitude=float(row['latitude']),
                    timezone_offset_sec=int(row['timezone_offset_sec']) if pd.notna(row['timezone_offset_sec']) else None,
                    first_seen=datetime.now(),
                    last_updated=datetime.now()
                )
                self.session.add(city)
            
            upserted += 1
        
        self.session.commit()
        logger.info(f" Upserted {upserted} cities")
    
    def load_weather_facts(self, df):
        """
        Load weather fact records (bulk insert)
        """
        logger.info("Loading weather facts...")
        
        # Add loaded_at timestamp
        df['loaded_at'] = datetime.now()
        
        # Convert DataFrame to dict records
        records = df.to_dict('records')
        
        # Bulk insert using pandas
        try:
            # Use pandas to_sql for efficient bulk loading
            df.to_sql(
                'weather_fact',
                self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            logger.info(f" Loaded {len(df)} weather fact records")
            return len(df)
        
        except Exception as e:
            logger.error(f"Error loading facts: {e}")
            return 0
    
    def log_load_history(self, records_count, source_file, status, error_msg=None, duration=0):
        """
        Log load operation to audit table
        """
        history = LoadHistory(
            load_timestamp=datetime.now(),
            records_loaded=records_count,
            source_file=source_file,
            status=status,
            error_message=error_msg,
            duration_seconds=duration
        )
        
        self.session.add(history)
        self.session.commit()
        logger.info(f" Load history logged: {status}")
    
    def get_load_statistics(self):
        """
        Get database statistics
        """
        stats = {}
        
        # Total records
        stats['total_weather_records'] = self.session.query(WeatherFact).count()
        stats['total_cities'] = self.session.query(CityDimension).count()
        stats['total_loads'] = self.session.query(LoadHistory).count()
        
        # Latest load
        latest_load = self.session.query(LoadHistory).order_by(
            LoadHistory.load_timestamp.desc()
        ).first()
        
        if latest_load:
            stats['latest_load_time'] = latest_load.load_timestamp
            stats['latest_load_records'] = latest_load.records_loaded
            stats['latest_load_status'] = latest_load.status
        
        # Date range
        result = self.session.execute(
            text("SELECT MIN(dt_datetime), MAX(dt_datetime) FROM weather_fact")
        ).fetchone()
        
        if result[0]:
            stats['earliest_reading'] = result[0]
            stats['latest_reading'] = result[1]
        
        return stats
    
    def close(self):
        """
        Close database session
        """
        self.session.close()
        logger.info("Database session closed")


def load_weather_to_database():
    """
    Main function to load data into PostgreSQL
    """
    loader = WeatherLoader()
    start_time = datetime.now()
    
    try:
        # 1. Load processed data
        print("\n Loading processed data...")
        df, source_file = loader.load_processed_data()
        
        if df is None:
            print(" No data to load!")
            return
        
        print(f"    Loaded {len(df)} records from {os.path.basename(source_file)}")
        
        # 2. Upsert city dimension
        print("\n  Updating city dimension...")
        loader.upsert_city_dimension(df)
        
        # 3. Load weather facts
        print("\n Loading weather facts...")
        records_loaded = loader.load_weather_facts(df)
        
        # 4. Log history
        duration = (datetime.now() - start_time).total_seconds()
        loader.log_load_history(
            records_count=records_loaded,
            source_file=source_file,
            status='success',
            duration=duration
        )
        
        # 5. Show statistics
        print("\n" + "=" * 70)
        print(" DATABASE STATISTICS")
        print("=" * 70)
        
        stats = loader.get_load_statistics()
        print(f"\n Total Records:")
        print(f"   • Weather readings: {stats['total_weather_records']}")
        print(f"   • Cities: {stats['total_cities']}")
        print(f"   • Load operations: {stats['total_loads']}")
        
        print(f"\n Data Coverage:")
        print(f"   • Earliest: {stats.get('earliest_reading', 'N/A')}")
        print(f"   • Latest: {stats.get('latest_reading', 'N/A')}")
        
        print(f"\n  This Load:")
        print(f"   • Records loaded: {records_loaded}")
        print(f"   • Duration: {duration:.2f} seconds")
        
        print("\n Data loading completed successfully!")
        
    except Exception as e:
        logger.error(f" Load failed: {e}")
        
        duration = (datetime.now() - start_time).total_seconds()
        loader.log_load_history(
            records_count=0,
            source_file='unknown',
            status='failed',
            error_msg=str(e),
            duration=duration
        )
        
        print(f"\n Error: {e}")
    
    finally:
        loader.close()