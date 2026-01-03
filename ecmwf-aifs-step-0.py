import xarray as xr
import datetime
from ecmwf.opendata import Client


# Configuration
LAT, LON = 51.5, 0.0
START_DATE = datetime.date(2025, 6, 1)
N_DAYS = 91
OUTPUT_FP = "data/aifs-forecast-step-0"


def process_dates(dates):
    client = Client(source="aws", model="aifs-single", resol="0p25",)

    # Requests the current time step of all forecast runs for a given date ranged
    request = {
        "date": dates,
        "time": [0, 6, 12, 18],
        "type": "fc",
        "step": 0,
        "param": "2t"
    }

    try:

        # Retrieves data and writes to grib2 file
        client.retrieve(request, f"{OUTPUT_FP}.grib2")

        # Load temp file into xarray and perform filtration by coord 
        # and data reshaping into dataframe before exporting as CSV
        with xr.open_dataset(f"{OUTPUT_FP}.grib2", engine="cfgrib", backend_kwargs={"indexpath": "","filter_by_keys": {'shortName': '2t'}}) as ds:
            ds_point = ds.sel(latitude=LAT, longitude=LON, method='nearest').load()
            df = (ds_point['t2m'] - 273.15).to_dataframe(name='temp_c')
            df = df.reset_index().drop(columns=['latitude', 'longitude', 'heightAboveGround'], errors='ignore')
            df.to_csv(f"{OUTPUT_FP}.csv", index=False)

        print("Task complete")
    except Exception as e:
        print(f"Task failed: {e}")

if __name__ == "__main__": 

    # Creates date range according to START_DATE and N_DAYS in configuration
    dates = [START_DATE + datetime.timedelta(days=idx) for idx in range(N_DAYS)]

    print(f"Downloading all forecasts for {N_DAYS} days from {START_DATE}")

    process_dates(dates)


