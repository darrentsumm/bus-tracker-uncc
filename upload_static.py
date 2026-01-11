import os
import pandas as pd
import passiogo
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 1. Load Environment Variables
load_dotenv()
db_url = os.getenv("DATABASE_URL")

if not db_url:
    raise ValueError("DATABASE_URL not found")

# 2. Connect to Neon
engine = create_engine(db_url)

SCHOOL_ID = 1053

def upload_routes():
    try:
        print("Fetching static routes...")
        system = passiogo.getSystemFromID(SCHOOL_ID)
        routes = system.getRoutes()
        
        # Convert to DataFrame
        routes_data = [r.__dict__ for r in routes]
        df = pd.DataFrame(routes_data)
        
        # Clean up
        if 'system' in df.columns:
            df = df.drop(columns=['system'])
            
        # Add Timestamp (Crucial for dbt!)
        df['load_timestamp'] = pd.Timestamp.now()

        # Upload - replace ensures we don't duplicate routes every run
        df.to_sql('raw_routes', engine, if_exists='replace', index=False)
        
        print(f"Success: Uploaded {len(df)} routes to 'raw_routes'.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    upload_routes()