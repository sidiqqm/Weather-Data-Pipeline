from src.transform.weather_transformer import transform_weather_data

if __name__ == "__main__":
    print("=" * 70)
    print(" WEATHER DATA TRANSFORMATION PIPELINE")
    print("=" * 70)
    
    df = transform_weather_data()
    
    if df is not None:
        print("\n" + "=" * 70)
        print(" SAMPLE TRANSFORMED DATA (First 5 rows)")
        print("=" * 70)
        print(df.head())
        
        print("\n" + "=" * 70)
        print(" COLUMN SUMMARY")
        print("=" * 70)
        print(f"Total columns: {len(df.columns)}")
        print("\nColumn names:")
        for i, col in enumerate(df.columns, 1):
            print(f"   {i:2}. {col}")