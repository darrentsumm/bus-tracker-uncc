import os
import time
import pandas as pd
import passiogo
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 1. Load Environment Variables
load_dotenv()
db_url = os.getenv("DATABASE_URL")

if not db_url:
    raise ValueError("DATABASE_URL not found in .env file")

# 2. Connect to Neon (Postgres) using SQLAlchemy
# This is much cleaner than the Snowflake connector!
engine = create_engine(db_url)

# Set the School ID (UNC Charlotte)
SCHOOL_ID = 1053 

def fetch_and_upload():
    try:
        # --- EXTRACT ---
        system = passiogo.getSystemFromID(SCHOOL_ID)
        vehicles = system.getVehicles()
        
        if not vehicles:
            print("No active vehicles found.")
            return

        # Convert to DataFrame
        # ... inside fetch_and_upload ...
        vehicles_data = [vehicle.__dict__ for vehicle in vehicles]
        df = pd.DataFrame(vehicles_data)
        
        # --- ADD THIS: Create the timestamp column ---
        df['load_timestamp'] = pd.Timestamp.now()
        
        # ... rest of code (dropping system, to_sql) ...
        
        # Cleanup: Drop the 'system' column if it exists (it's an object we can't upload)
        if 'system' in df.columns:
            df = df.drop(columns=['system'])

        # --- LOAD ---
        # "if_exists='append'" adds new rows. 
        # "index=False" ensures we don't upload the row numbers.
        df.to_sql('raw_vehicles', engine, if_exists='append', index=False)
        
        print(f"Success: Uploaded {len(df)} rows to 'raw_vehicles' table.")

    except Exception as e:
        print(f"Error: {e}")


def lambda_handler(event, context):
    print("Lambda started...")
    fetch_and_upload()
    return {
        'statusCode': 200,
        'body': 'Bus data uploaded successfully!'
    }

# This allows you to still test it locally on your laptop
if __name__ == "__main__":
    lambda_handler(None, None)