import requests
import pandas as pd

BASE_URL = "https://api.openf1.org/v1"

def fetch_endpoint(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# Contoh mengambil data laps session tertentu
params = {
    "session_key": "9161",
    "driver_number": "63"
}
lap_data = fetch_endpoint("laps", params)

# Convert ke pandas DataFrame agar mudah lihat
df_laps = pd.DataFrame(lap_data)
print(df_laps.head())