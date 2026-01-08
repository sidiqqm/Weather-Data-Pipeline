from src.extract.weather_extractor import extract_weather_data

# List kota-kota
cities = [
    'Jakarta',
    'Surabaya',
    'Bandung',
    'Medan',
    'Semarang',
    'Makassar',
    'Palembang',
    'Denpasar',
    'Singapore',
    'Kuala Lumpur',
    'Bangkok',
    'Manila',
    'Ho Chi Minh City',
    'Hanoi',
    'Yangon'
]

if __name__ == "__main__":
    print("=" * 60)
    print("WEATHER DATA EXTRACTION PIPELINE")
    print("=" * 60)
    print(f"\n Extracting weather data for {len(cities)} cities...")
    print("-" * 60)
    
    # Run extraction
    raw_data = extract_weather_data(cities)
    
    if raw_data:
        print("\n Extraction completed successfully!")
        print("\n Check the following:")
        print("   • data/raw/ folder for JSON files")
        print("   • logs/extraction.log for detailed logs")
    else:
        print("\n Extraction failed!")