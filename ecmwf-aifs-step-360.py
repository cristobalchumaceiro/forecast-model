import xarray as xr
import tempfile
import os
import datetime
from ecmwf.opendata import Client
from concurrent.futures import ProcessPoolExecutor

# Configuration
MAX_WORKERS = 4 
LAT, LON = 51.5, 0.0
START_DATE = datetime.date(2025, 6, 1)
N_DAYS = 91
OUTPUT_DIR = "data/aifs-forecast-step-360/csv"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def process_date(date):
    client = Client(source="aws", model="aifs-single", resol="0p25")
    
    # Requests all time steps of all forecast runs for a given date
    request = {
        "date": date,
        "time": [0, 6, 12, 18],
        "type": "fc",
        "step": list(range(0, 361, 6)),
        "param": "2t"
    }

    try:

        # client.retrieve() requires a file path instead of a memory buffer
        with tempfile.NamedTemporaryFile(suffix=f"_{date}.grib2", delete=True) as tmp:
            client.retrieve(request, tmp.name)

            # Load temp file into xarray and perform filtration by coord 
            # and data reshaping into dataframe before exporting as CSV
            with xr.open_dataset(tmp.name, engine="cfgrib", backend_kwargs={"indexpath": "","filter_by_keys": {'shortName': '2t'}}) as ds:
                ds_point = ds.sel(latitude=LAT, longitude=LON, method='nearest').load()
                df = (ds_point['t2m'] - 273.15).to_dataframe(name='temp_c')
                df = df.reset_index().drop(columns=['latitude', 'longitude', 'heightAboveGround'], errors='ignore')
                df.to_csv(f"{OUTPUT_DIR}/{date}.csv", index=False)

        print(f"{date} complete")
    except Exception as e:
        print(f"{date} failed: {e}")

if __name__ == "__main__":

    # Creates date range according to START_DATE and N_DAYS in configuration
    dates = [START_DATE + datetime.timedelta(days=idx) for idx in range(N_DAYS)]

    print(f"Starting Run ({MAX_WORKERS} workers)...")

    # Downloads data on separate processes according to MAX_WORKERS, avoiding memory leaks  
    with ProcessPoolExecutor(max_workers=MAX_WORKERS, max_tasks_per_child=1) as executor:
        executor.map(process_date, dates)
        
    print("All tasks complete.")
                