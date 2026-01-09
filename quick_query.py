import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql://weather_user:weather_pass123@localhost:5432/weather_analytics"
)

print("=" * 70)
print("🔍 QUICK DATA ANALYSIS")
print("=" * 70)

# Custom query
custom_query = """
SELECT 
    city_name,
    country_code,
    temp_celsius,
    humidity_percent,
    wind_speed_mps,
    weather_main,
    weather_severity
FROM weather_fact
WHERE temp_celsius > 25
ORDER BY weather_severity DESC
LIMIT 10;
"""

print("\n📊 Cities with Temperature > 25°C (sorted by severity):")
print("-" * 70)

df = pd.read_sql(custom_query, engine)
print(df.to_string(index=False))

print("\n" + "=" * 70)
print("💡 Modify the query in quick_query.py to explore different insights!")
print("=" * 70)