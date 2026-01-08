import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres123')
}

DB_NAME = 'weather_analytics'
DB_USER = 'weather_user'
DB_PASSWORD = 'weather_pass123'

def setup_database():
    """
    Create database and user for weather pipeline
    """
    print("=" * 70)
    print(" POSTGRESQL DATABASE SETUP")
    print("=" * 70)
    
    try:
        # Connect to default postgres database
        print("\n Connecting to PostgreSQL server...")
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", 
            (DB_NAME,)
        )
        db_exists = cursor.fetchone()
        
        if db_exists:
            print(f"  Database '{DB_NAME}' already exists. Dropping and recreating...")
            # Terminate existing connections
            cursor.execute(f"""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = '{DB_NAME}'
                AND pid <> pg_backend_pid();
            """)
            cursor.execute(sql.SQL("DROP DATABASE {}").format(
                sql.Identifier(DB_NAME)
            ))
        
        # Create database
        print(f"\n Creating database '{DB_NAME}'...")
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(
            sql.Identifier(DB_NAME)
        ))
        print(" Database created successfully!")
        
        # Check if user exists
        cursor.execute(
            "SELECT 1 FROM pg_roles WHERE rolname = %s", 
            (DB_USER,)
        )
        user_exists = cursor.fetchone()
        
        if user_exists:
            print(f"  User '{DB_USER}' already exists. Dropping and recreating...")
            cursor.execute(sql.SQL("DROP USER {}").format(
                sql.Identifier(DB_USER)
            ))
        
        # Create user
        print(f"\n👤 Creating user '{DB_USER}'...")
        cursor.execute(sql.SQL(
            "CREATE USER {} WITH PASSWORD %s"
        ).format(sql.Identifier(DB_USER)), [DB_PASSWORD])
        print(" User created successfully!")
        
        # Grant privileges
        print(f"\n Granting privileges to '{DB_USER}'...")
        cursor.execute(sql.SQL(
            "GRANT ALL PRIVILEGES ON DATABASE {} TO {}"
        ).format(
            sql.Identifier(DB_NAME),
            sql.Identifier(DB_USER)
        ))
        print(" Privileges granted!")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 70)
        print(" DATABASE SETUP COMPLETED!")
        print("=" * 70)
        print(f"\n Database Details:")
        print(f"   • Database: {DB_NAME}")
        print(f"   • User: {DB_USER}")
        print(f"   • Password: {DB_PASSWORD}")
        print(f"   • Host: localhost")
        print(f"   • Port: 5432")
        
        return True
        
    except psycopg2.Error as e:
        print(f"\n Database error: {e}")
        return False
    except Exception as e:
        print(f"\n Unexpected error: {e}")
        return False

if __name__ == "__main__":
    setup_database()