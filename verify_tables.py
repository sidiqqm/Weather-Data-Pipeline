import psycopg2
from tabulate import tabulate

try:
    from tabulate import tabulate
except ImportError:
    print("Installing tabulate...")
    import subprocess
    subprocess.check_call(['pip', 'install', 'tabulate'])
    from tabulate import tabulate

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='weather_analytics',
    user='weather_user',
    password='weather_pass123'
)

cursor = conn.cursor()

print("=" * 70)
print(" VERIFYING DATABASE TABLES")
print("=" * 70)

# List all tables
cursor.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public'
    ORDER BY table_name;
""")

tables = cursor.fetchall()

if tables:
    print(f"\n Found {len(tables)} tables:")
    for i, (table,) in enumerate(tables, 1):
        print(f"   {i}. {table}")
        
        # Get column info for each table
        cursor.execute(f"""
            SELECT 
                column_name, 
                data_type,
                is_nullable
            FROM information_schema.columns 
            WHERE table_name = '{table}'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        print(f"\n       Columns in '{table}': {len(columns)}")
        
        # Show first 5 columns
        print(f"      First 5 columns:")
        for col_name, col_type, nullable in columns[:5]:
            null_info = "NULL" if nullable == "YES" else "NOT NULL"
            print(f"         • {col_name:<30} {col_type:<20} {null_info}")
        
        if len(columns) > 5:
            print(f"         ... and {len(columns) - 5} more columns")
        print()
else:
    print("\n No tables found!")

cursor.close()
conn.close()

print("=" * 70)
print(" Verification complete!")
print("=" * 70)