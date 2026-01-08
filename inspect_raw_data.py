import json
import os
from glob import glob

# Find the latest raw data file
raw_files = glob('data/raw/weather_raw_*.json')
if not raw_files:
    print("No raw data files found!")
    exit()

latest_file = max(raw_files, key=os.path.getctime)
print(f"Inspecting: {latest_file}\n")

# Load and display
with open(latest_file, 'r') as f:
    data = json.load(f)

print(f"Total records: {len(data)}")
print("\n" + "="*60)
print("Sample Record (First City):")
print("="*60)
print(json.dumps(data[0], indent=2))

print("\n" + "="*60)
print("Available Fields:")
print("="*60)
for key in data[0].keys():
    print(f"   • {key}")