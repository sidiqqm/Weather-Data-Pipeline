from src.load.weather_loader import load_weather_to_database

if __name__ == "__main__":
    print("=" * 70)
    print(" LOADING DATA TO POSTGRESQL")
    print("=" * 70)
    
    load_weather_to_database()