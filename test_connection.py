import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get API key
API_KEY = os.getenv('OPENWEATHER_API_KEY')

# Test API call
url = f"http://api.openweathermap.org/data/2.5/weather?q=Jakarta&appid={API_KEY}"
response = requests.get(url)

if response.status_code == 200:
    print(" Connection successful!")
    print(f"Data sample: {response.json()['name']}, {response.json()['main']['temp']}K")
else:
    print(f" Error: {response.status_code}")