import urllib.request
import json
import pandas as pd
import os
from sqlalchemy import create_engine
from datetime import datetime

def lambda_handler(event, context):
    print("Fetching live vehicle data directly from API...")
    
    # Hit the Passio GO raw data feed for UNCC (System 1053)
    url = "https://passiogo.com/mapGetData.php?getBuses=1&deviceId=0&system=1053"
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    
    try:
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
    except Exception as e:
        print(f"API Error: {e}")
        return
        
    # The API returns a dictionary of buses. Convert it to a list.
    buses_dict = data.get("buses", {})
    bus_list = list(buses_dict.values())
    
    if not bus_list:
        print("No active vehicles found.")
        return
        
    # Create DataFrame
    df = pd.DataFrame(bus_list)
    print(f"Found {len(df)} buses.")
    
    # Add timestamp
    df['load_timestamp'] = datetime.utcnow()
    
    # Upload to Neon 
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("Error: DATABASE_URL environment variable is missing.")
        return
        
    engine = create_engine(db_url)
    
    # Use 'append' so the new table grows over time
    df.to_sql('raw_vehicles', engine, if_exists='append', index=False)
    
    print("Successfully built table and uploaded data.")

# For local testing
if __name__ == "__main__":
    lambda_handler(None, None)