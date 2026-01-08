import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv
import time
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class WeatherExtractor:
    """
    Class untuk extract data cuaca dari OpenWeatherMap API
    """
    
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        self.base_url = "http://api.openweathermap.org/data/2.5/weather"
        
        if not self.api_key:
            raise ValueError("API key not found! Check your .env file")
        
        logger.info("WeatherExtractor initialized")
    
    def extract_city_weather(self, city_name):
        """
        Extract weather data untuk satu kota
        
        Args:
            city_name (str): Nama kota
            
        Returns:
            dict: Raw weather data atau None jika error
        """
        try:
            params = {
                'q': city_name,
                'appid': self.api_key,
                'units': 'metric'  # Celsius
            }
            
            logger.info(f"Fetching weather data for {city_name}")
            response = requests.get(self.base_url, params=params, timeout=10)
            
            # Check response status
            if response.status_code == 200:
                data = response.json()
                # Add extraction metadata
                data['extracted_at'] = datetime.now().isoformat()
                data['extraction_source'] = 'openweathermap_api'
                
                logger.info(f" Successfully fetched data for {city_name}")
                return data
            
            elif response.status_code == 404:
                logger.warning(f" City not found: {city_name}")
                return None
            
            elif response.status_code == 429:
                logger.error(" Rate limit exceeded! Wait before retrying")
                return None
            
            else:
                logger.error(f" Error {response.status_code} for {city_name}")
                return None
                
        except requests.exceptions.Timeout:
            logger.error(f" Timeout error for {city_name}")
            return None
        
        except requests.exceptions.RequestException as e:
            logger.error(f" Request error for {city_name}: {str(e)}")
            return None
        
        except Exception as e:
            logger.error(f" Unexpected error for {city_name}: {str(e)}")
            return None
    
    def extract_multiple_cities(self, city_list, delay=1):
        """
        Extract weather data untuk multiple cities
        
        Args:
            city_list (list): List of city names
            delay (int): Delay between requests (seconds) untuk menghindari rate limit
            
        Returns:
            list: List of weather data dictionaries
        """
        all_data = []
        
        logger.info(f"Starting batch extraction for {len(city_list)} cities")
        
        for i, city in enumerate(city_list, 1):
            logger.info(f"Processing {i}/{len(city_list)}: {city}")
            
            weather_data = self.extract_city_weather(city)
            
            if weather_data:
                all_data.append(weather_data)
            
            # Delay to respect rate limits (free tier: 60 calls/minute)
            if i < len(city_list):
                time.sleep(delay)
        
        logger.info(f" Batch extraction completed: {len(all_data)}/{len(city_list)} successful")
        return all_data
    
    def save_raw_data(self, data, filename=None):
        """
        Save raw data ke JSON file
        
        Args:
            data (list): List of weather data
            filename (str): Custom filename (optional)
        """
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"weather_raw_{timestamp}.json"
        
        filepath = os.path.join('data', 'raw', filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f" Raw data saved to {filepath}")
            return filepath
        
        except Exception as e:
            logger.error(f" Error saving raw data: {str(e)}")
            return None


# Helper function untuk mudah digunakan
def extract_weather_data(cities):
    """
    Main function untuk extract weather data
    """
    extractor = WeatherExtractor()
    raw_data = extractor.extract_multiple_cities(cities)
    
    if raw_data:
        filepath = extractor.save_raw_data(raw_data)
        print(f"\n Extraction Summary:")
        print(f"   • Cities processed: {len(cities)}")
        print(f"   • Successful extractions: {len(raw_data)}")
        print(f"   • Raw data saved to: {filepath}")
        return raw_data
    else:
        print(" No data extracted")
        return None