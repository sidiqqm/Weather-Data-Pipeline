import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres123')
}

DB_NAME = 'weather_analytics'
DB_USER = 'weather_user'

def fix_permissions():
    """
    Grant proper permissions to weather_user
    """
    print("=" * 70)
    print(" FIXING POSTGRESQL PERMISSIONS")
    print("=" * 70)
    
    try:
        # Connect as superuser (postgres) to target database
        conn = psycopg2.connect(
            **DB_CONFIG,
            database=DB_NAME
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        print(f"\n Connected to database '{DB_NAME}'")
        
        # Grant schema permissions
        print(f"\n Granting schema permissions to '{DB_USER}'...")
        cursor.execute(f"""
            GRANT ALL ON SCHEMA public TO {DB_USER};
        """)
        print(" Schema permissions granted")
        
        # Grant usage and create privileges
        print(f"\n Granting usage and create privileges...")
        cursor.execute(f"""
            GRANT USAGE, CREATE ON SCHEMA public TO {DB_USER};
        """)
        print(" Usage and create privileges granted")
        
        # Grant all privileges on all tables (if any exist)
        print(f"\n Granting table privileges...")
        cursor.execute(f"""
            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {DB_USER};
        """)
        print(" Table privileges granted")
        
        # Grant all privileges on all sequences (for auto-increment)
        print(f"\n Granting sequence privileges...")
        cursor.execute(f"""
            GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {DB_USER};
        """)
        print(" Sequence privileges granted")
        
        # Set default privileges for future tables
        print(f"\n Setting default privileges for future objects...")
        cursor.execute(f"""
            ALTER DEFAULT PRIVILEGES IN SCHEMA public
            GRANT ALL ON TABLES TO {DB_USER};
        """)
        cursor.execute(f"""
            ALTER DEFAULT PRIVILEGES IN SCHEMA public
            GRANT ALL ON SEQUENCES TO {DB_USER};
        """)
        print(" Default privileges set")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 70)
        print(" PERMISSIONS FIXED SUCCESSFULLY!")
        print("=" * 70)
        
        return True
        
    except psycopg2.Error as e:
        print(f"\n Database error: {e}")
        return False
    except Exception as e:
        print(f"\n Unexpected error: {e}")
        return False

if __name__ == "__main__":
    success = fix_permissions()
    
    if success:
        print("\nRun 'python src/load/database_schema.py' again")
    else:
        print("\n Permission fix failed. Check the error message above.")