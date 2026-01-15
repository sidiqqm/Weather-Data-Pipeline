import pandas as pd
import json
import os
from datetime import datetime
from glob import glob
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/transformation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class WeatherTransformer:
    """
    Class untuk transform raw weather data menjadi structured format
    """
    
    def __init__(self):
        logger.info("WeatherTransformer initialized")
    
    def load_raw_data(self, filepath=None):
        """
        Load raw JSON data
        
        Args:
            filepath (str): Path to raw JSON file. If None, loads latest.
            
        Returns:
            list: Raw weather data
        """
        if filepath is None:
            # Find latest raw file
            raw_files = glob('data/raw/weather_raw_*.json')
            if not raw_files:
                logger.error(" No raw data files found!")
                return None
            filepath = max(raw_files, key=os.path.getctime)
        
        logger.info(f"Loading raw data from: {filepath}")
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f" Loaded {len(data)} records")
            return data
        except Exception as e:
            logger.error(f" Error loading raw data: {str(e)}")
            return None
    
    def flatten_weather_data(self, raw_data):
        """
        Flatten nested JSON structure menjadi tabular format
        
        Args:
            raw_data (list): List of raw weather dictionaries
            
        Returns:
            pd.DataFrame: Flattened data
        """
        logger.info("Starting data flattening...")
        
        flattened_records = []
        
        for record in raw_data:
            try:
                # Extract main fields
                flat_record = {
                    # Identifiers
                    'city_id': record.get('id'),
                    'city_name': record.get('name'),
                    'country_code': record.get('sys', {}).get('country'),
                    
                    # Coordinates
                    'longitude': record.get('coord', {}).get('lon'),
                    'latitude': record.get('coord', {}).get('lat'),
                    
                    # Weather condition (ambil yang pertama dari array)
                    'weather_id': record.get('weather', [{}])[0].get('id'),
                    'weather_main': record.get('weather', [{}])[0].get('main'),
                    'weather_description': record.get('weather', [{}])[0].get('description'),
                    'weather_icon': record.get('weather', [{}])[0].get('icon'),
                    
                    # Temperature & feels like
                    'temp_celsius': record.get('main', {}).get('temp'),
                    'feels_like_celsius': record.get('main', {}).get('feels_like'),
                    'temp_min_celsius': record.get('main', {}).get('temp_min'),
                    'temp_max_celsius': record.get('main', {}).get('temp_max'),
                    
                    # Pressure & humidity
                    'pressure_hpa': record.get('main', {}).get('pressure'),
                    'humidity_percent': record.get('main', {}).get('humidity'),
                    'sea_level_hpa': record.get('main', {}).get('sea_level'),
                    'ground_level_hpa': record.get('main', {}).get('grnd_level'),
                    
                    # Wind
                    'wind_speed_mps': record.get('wind', {}).get('speed'),
                    'wind_direction_deg': record.get('wind', {}).get('deg'),
                    'wind_gust_mps': record.get('wind', {}).get('gust'),
                    
                    # Clouds & visibility
                    'cloudiness_percent': record.get('clouds', {}).get('all'),
                    'visibility_meters': record.get('visibility'),
                    
                    # Rain & snow (if available)
                    'rain_1h_mm': record.get('rain', {}).get('1h', 0),
                    'rain_3h_mm': record.get('rain', {}).get('3h', 0),
                    'snow_1h_mm': record.get('snow', {}).get('1h', 0),
                    'snow_3h_mm': record.get('snow', {}).get('3h', 0),
                    
                    # Timestamps
                    'dt_unix': record.get('dt'),
                    'timezone_offset_sec': record.get('timezone'),
                    'sunrise_unix': record.get('sys', {}).get('sunrise'),
                    'sunset_unix': record.get('sys', {}).get('sunset'),
                    
                    # Metadata
                    'extracted_at': record.get('extracted_at'),
                    'extraction_source': record.get('extraction_source')
                }
                
                flattened_records.append(flat_record)
                
            except Exception as e:
                logger.warning(f" Error flattening record for {record.get('name')}: {str(e)}")
                continue
        
        df = pd.DataFrame(flattened_records)
        logger.info(f" Flattened {len(df)} records with {len(df.columns)} columns")
        
        return df
    
    def clean_data(self, df):
        """
        Clean and validate data
        
        Args:
            df (pd.DataFrame): Flattened dataframe
            
        Returns:
            pd.DataFrame: Cleaned dataframe
        """
        logger.info("Starting data cleaning...")
        
        df_clean = df.copy()
        initial_rows = len(df_clean)
        
        # 1. Remove duplicates based on city_id and extraction time
        df_clean = df_clean.drop_duplicates(subset=['city_id', 'extracted_at'])
        logger.info(f"   • Removed {initial_rows - len(df_clean)} duplicates")
        
        # 2. Handle missing critical values
        # Remove records without city name or coordinates
        df_clean = df_clean.dropna(subset=['city_name', 'latitude', 'longitude'])
        logger.info(f"   • Removed {initial_rows - len(df_clean)} rows with missing critical fields")
        
        # 3. Data type conversions
        numeric_columns = [
            'longitude', 'latitude', 'temp_celsius', 'feels_like_celsius',
            'temp_min_celsius', 'temp_max_celsius', 'pressure_hpa', 
            'humidity_percent', 'wind_speed_mps', 'cloudiness_percent'
        ]
        
        for col in numeric_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        
        # 4. Convert timestamps to datetime
        df_clean['dt_datetime'] = pd.to_datetime(df_clean['dt_unix'], unit='s')
        df_clean['sunrise_datetime'] = pd.to_datetime(df_clean['sunrise_unix'], unit='s')
        df_clean['sunset_datetime'] = pd.to_datetime(df_clean['sunset_unix'], unit='s')
        df_clean['extracted_at_datetime'] = pd.to_datetime(df_clean['extracted_at'])
        
        # 5. Validate reasonable ranges
        # Temperature: -50 to 60 Celsius
        df_clean = df_clean[
            (df_clean['temp_celsius'] >= -50) & 
            (df_clean['temp_celsius'] <= 60)
        ]
        
        # Humidity: 0 to 100%
        df_clean = df_clean[
            (df_clean['humidity_percent'] >= 0) & 
            (df_clean['humidity_percent'] <= 100)
        ]
        
        logger.info(f" Cleaning completed: {len(df_clean)} valid records")
        
        return df_clean
    
    def enrich_data(self, df):
        """
        Add derived/calculated columns for better analytics
        
        Args:
            df (pd.DataFrame): Cleaned dataframe
            
        Returns:
            pd.DataFrame: Enriched dataframe
        """
        logger.info("Starting data enrichment...")
        
        df_enriched = df.copy()
        
        # 1. Temperature category
        def categorize_temp(temp):
            if pd.isna(temp):
                return 'Unknown'
            elif temp < 10:
                return 'Cold'
            elif temp < 20:
                return 'Cool'
            elif temp < 30:
                return 'Warm'
            else:
                return 'Hot'
        
        df_enriched['temp_category'] = df_enriched['temp_celsius'].apply(categorize_temp)
        
        # 2. Humidity category
        def categorize_humidity(humidity):
            if pd.isna(humidity):
                return 'Unknown'
            elif humidity < 30:
                return 'Dry'
            elif humidity < 60:
                return 'Comfortable'
            else:
                return 'Humid'
        
        df_enriched['humidity_category'] = df_enriched['humidity_percent'].apply(categorize_humidity)
        
        # 3. Wind strength category
        def categorize_wind(speed):
            if pd.isna(speed):
                return 'Unknown'
            elif speed < 5:
                return 'Calm'
            elif speed < 10:
                return 'Moderate'
            else:
                return 'Strong'
        
        df_enriched['wind_strength'] = df_enriched['wind_speed_mps'].apply(categorize_wind)
        
        # 4. Calculate daylight duration (in hours)
        df_enriched['daylight_hours'] = (
            (df_enriched['sunset_datetime'] - df_enriched['sunrise_datetime'])
            .dt.total_seconds() / 3600
        )
        
        # 5. Heat index approximation (feels like vs actual temp difference)
        df_enriched['heat_index_diff'] = (
            df_enriched['feels_like_celsius'] - df_enriched['temp_celsius']
        )
        
        # 6. Is it raining or snowing?
        df_enriched['is_raining'] = (
            (df_enriched['rain_1h_mm'] > 0) | (df_enriched['rain_3h_mm'] > 0)
        )
        df_enriched['is_snowing'] = (
            (df_enriched['snow_1h_mm'] > 0) | (df_enriched['snow_3h_mm'] > 0)
        )
        
        # 7. Weather severity score (simple scoring)
        # Higher = more extreme weather
        df_enriched['weather_severity'] = (
            abs(df_enriched['temp_celsius'] - 25) / 10 +  # Temperature deviation from comfortable
            df_enriched['wind_speed_mps'] / 5 +             # Wind speed
            (100 - df_enriched['humidity_percent']) / 50 +  # Humidity deviation
            df_enriched['cloudiness_percent'] / 100         # Cloudiness
        ).round(2)
        
        # 8. Extract date components for time-based analysis
        df_enriched['date'] = df_enriched['dt_datetime'].dt.date
        df_enriched['hour'] = df_enriched['dt_datetime'].dt.hour
        df_enriched['day_of_week'] = df_enriched['dt_datetime'].dt.day_name()
        df_enriched['month'] = df_enriched['dt_datetime'].dt.month_name()
        
        logger.info(f"Added {len([c for c in df_enriched.columns if c not in df.columns])} new derived columns")
        
        return df_enriched
    
    def generate_data_quality_report(self, df):
        """
        Generate data quality report
        
        Args:
            df (pd.DataFrame): Dataframe to analyze
        """
        logger.info("Generating data quality report...")
        
        report = {
            'total_records': len(df),
            'total_columns': len(df.columns),
            'missing_values': df.isnull().sum().to_dict(),
            'duplicate_cities': df['city_name'].duplicated().sum(),
            'unique_cities': df['city_name'].nunique(),
            'unique_countries': df['country_code'].nunique(),
            'date_range': {
                'earliest': str(df['dt_datetime'].min()),
                'latest': str(df['dt_datetime'].max())
            },
            'numeric_stats': df.describe().to_dict()
        }
        
        return report
    
    def save_transformed_data(self, df, format='csv'):
        """
        Save transformed data
        
        Args:
            df (pd.DataFrame): Transformed dataframe
            format (str): 'csv' or 'parquet'
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if format == 'csv':
            filepath = f"data/processed/weather_clean_{timestamp}.csv"
            df.to_csv(filepath, index=False)
        elif format == 'parquet':
            filepath = f"data/processed/weather_clean_{timestamp}.parquet"
            df.to_parquet(filepath, index=False)
        else:
            raise ValueError("Format must be 'csv' or 'parquet'")
        
        logger.info(f" Transformed data saved to: {filepath}")
        return filepath


def transform_weather_data():
    """
    Main function untuk transform pipeline
    """
    transformer = WeatherTransformer()
    
    # 1. Load raw data
    print("\n Loading raw data...")
    raw_data = transformer.load_raw_data()
    if not raw_data:
        return None
    
    # 2. Flatten
    print(" Flattening nested structure...")
    df_flat = transformer.flatten_weather_data(raw_data)
    print(f"    Shape: {df_flat.shape}")
    
    # 3. Clean
    print(" Cleaning data...")
    df_clean = transformer.clean_data(df_flat)
    print(f"    Shape after cleaning: {df_clean.shape}")
    
    # 4. Enrich
    print(" Enriching data with derived features...")
    df_final = transformer.enrich_data(df_clean)
    print(f"    Final shape: {df_final.shape}")
    print(f"    Total columns: {len(df_final.columns)}")
    
    # 5. Data quality report
    print("\n Generating data quality report...")
    report = transformer.generate_data_quality_report(df_final)
    print(f"   • Total records: {report['total_records']}")
    print(f"   • Unique cities: {report['unique_cities']}")
    print(f"   • Countries: {report['unique_countries']}")
    
    # 6. Save
    print("\n Saving transformed data...")
    csv_path = transformer.save_transformed_data(df_final, format='csv')
    parquet_path = transformer.save_transformed_data(df_final, format='parquet')
    
    print(f"\n Transformation completed!")
    print(f"   • CSV: {csv_path}")
    print(f"   • Parquet: {parquet_path}")
    
    return df_final