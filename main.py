from ecmwf.opendata import Client
import datetime
import xarray as xr

# Setup Dates
td = datetime.date(2025, 6, 1)
k = 91
dates = [td + datetime.timedelta(days=idx) for idx in range(k)]

client = Client(
    source="aws",
    model="aifs-single",
    resol="0p25",
    preserve_request_order=True,
    infer_stream_keyword=True
)

request = {
    "date": dates,
    "time": [0, 6, 12, 18],
    "type": "fc",
    "step": 0,
    "param": "2t"
}

# Download file
filename = "data/data.grib2"
client.retrieve(request, filename)

# Load into Xarray
ds = xr.open_dataset(
    "data/data.grib2", 
    engine="cfgrib", 
    backend_kwargs={"indexpath": ""},
)

# Filter by coordinates 
ds_point = ds.sel(latitude=51.5, longitude=0.0, method='nearest').compute()

# Convert Kelvin to Celsius and drop columns
ds_point['t2m_c'] = ds_point['t2m'] - 273.15
ds_point = ds_point.drop_vars(['heightAboveGround', 'step'])

# Export to CSV
df = ds_point.to_dataframe()
df.to_csv("data/data.csv")



