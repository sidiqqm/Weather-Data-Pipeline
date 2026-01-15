import json
from glob import glob
import os

# Load latest raw data
raw_files = glob('data/raw/weather_raw_*.json')
latest_file = max(raw_files, key=os.path.getctime)

with open(latest_file, 'r') as f:
    data = json.load(f)

print("="*70)
print(" RAW DATA STRUCTURE ANALYSIS")
print("="*70)

# Sample record
sample = data[0]

print("\n📋 Main Keys:")
for key in sample.keys():
    value_type = type(sample[key]).__name__
    print(f"   • {key:<20} → {value_type}")

print("\n Nested 'coord' structure:")
print(f"   {json.dumps(sample['coord'], indent=6)}")

print("\n  Nested 'weather' structure:")
print(f"   {json.dumps(sample['weather'], indent=6)}")

print("\n  Nested 'main' structure:")
print(f"   {json.dumps(sample['main'], indent=6)}")

print("\n Nested 'wind' structure:")
print(f"   {json.dumps(sample['wind'], indent=6)}")

print("\n  Nested 'clouds' structure:")
print(f"   {json.dumps(sample['clouds'], indent=6)}")

print("\n Nested 'sys' structure:")
print(f"   {json.dumps(sample['sys'], indent=6)}")