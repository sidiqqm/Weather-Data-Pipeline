import pandas as pd
from sqlalchemy import create_engine, text

# Create engine
engine = create_engine(
    "postgresql://weather_user:weather_pass123@localhost:5432/weather_analytics"
)

print("=" * 80)
print("QUERYING POSTGRESQL DATABASE")
print("=" * 80)

# Query 1: Top 5 hottest cities
print("\n TOP 5 HOTTEST CITIES:")
print("-" * 80)
query1 = """
SELECT 
    city_name,
    country_code,
    temp_celsius,
    feels_like_celsius,
    humidity_percent,
    weather_main
FROM weather_fact
ORDER BY temp_celsius DESC
LIMIT 5;
"""
df1 = pd.read_sql(query1, engine)
print(df1.to_string(index=False))

# Query 2: Average temperature by country
print("\n\n AVERAGE TEMPERATURE BY COUNTRY:")
print("-" * 80)
query2 = """
SELECT 
    country_code,
    COUNT(*) as city_count,
    ROUND(AVG(temp_celsius)::numeric, 2) as avg_temp,
    ROUND(AVG(humidity_percent)::numeric, 2) as avg_humidity
FROM weather_fact
GROUP BY country_code
ORDER BY avg_temp DESC;
"""
df2 = pd.read_sql(query2, engine)
print(df2.to_string(index=False))

# Query 3: Weather condition distribution
print("\n\n WEATHER CONDITION DISTRIBUTION:")
print("-" * 80)
query3 = """
SELECT 
    weather_main,
    COUNT(*) as count,
    ROUND(AVG(temp_celsius)::numeric, 2) as avg_temp
FROM weather_fact
GROUP BY weather_main
ORDER BY count DESC;
"""
df3 = pd.read_sql(query3, engine)
print(df3.to_string(index=False))

# Query 4: Cities with rain
print("\n\n CITIES WITH RAIN:")
print("-" * 80)
query4 = """
SELECT 
    city_name,
    country_code,
    rain_1h_mm,
    rain_3h_mm,
    weather_description
FROM weather_fact
WHERE is_raining = true
ORDER BY rain_1h_mm DESC;
"""
df4 = pd.read_sql(query4, engine)
if len(df4) > 0:
    print(df4.to_string(index=False))
else:
    print("   No rainy cities in current dataset")

print("\n" + "=" * 80)
print("Query completed!")
print("=" * 80)