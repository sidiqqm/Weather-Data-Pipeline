import pandas as pd
from glob import glob
import os

# Load latest processed file
processed_files = glob('data/processed/weather_clean_*.csv')
if not processed_files:
    print(" No processed data files found!")
    exit()

latest_file = max(processed_files, key=os.path.getctime)
print(f"📂 Inspecting: {latest_file}\n")

# Load data
df = pd.read_csv(latest_file)

print("=" * 80)
print("DATA OVERVIEW")
print("=" * 80)
print(f"Shape: {df.shape[0]} rows × {df.shape[1]} columns\n")

print("=" * 80)
print("TEMPERATURE STATISTICS")
print("=" * 80)
print(df[['city_name', 'temp_celsius', 'temp_category', 'feels_like_celsius']].to_string(index=False))

print("\n" + "=" * 80)
print("WIND & HUMIDITY")
print("=" * 80)
print(df[['city_name', 'wind_speed_mps', 'wind_strength', 'humidity_percent', 'humidity_category']].to_string(index=False))

print("\n" + "=" * 80)
print("CONDITIONS")
print("=" * 80)
print(df[['city_name', 'weather_main', 'weather_description', 'cloudiness_percent']].to_string(index=False))

print("\n" + "=" * 80)
print("DERIVED METRICS")
print("=" * 80)
print(df[['city_name', 'weather_severity', 'daylight_hours', 'is_raining']].to_string(index=False))

print("\n" + "=" * 80)
print("DATA QUALITY CHECK")
print("=" * 80)
print(f"Missing values per column:")
missing = df.isnull().sum()
print(missing[missing > 0] if missing.sum() > 0 else "   No missing values!")