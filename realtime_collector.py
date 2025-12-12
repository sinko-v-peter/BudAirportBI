import time
import requests
import json
import os
from datetime import datetime

# --- KONFIGURÁCIÓ ---
# Ide mentjük a JSON fájlokat
DATA_FOLDER = r"C:\Users\SinkoGraphy\ME\School\uzleti_intelligencia_hf\BudAirportBI\Data"
RAW_FOLDER = os.path.join(DATA_FOLDER, "RealTime_JSON")

# Létrehozzuk a mappát, ha nem létezik
if not os.path.exists(RAW_FOLDER):
    os.makedirs(RAW_FOLDER)

# BKK API URL (A 100E Repülőtéri busz járműveit kérjük le)
# A 'bkk-web' kulcsot használjuk, ami publikus.
ROUTE_ID = "BKK_1005" # 100E járat belső azonosítója
URL = f"https://futar.bkk.hu/api/query/v1/ws/otp/api/where/vehicles-for-route.json?routeId={ROUTE_ID}&related=false&key=bkk-web&appVersion=1.1.abc"

print(f"--- BKK Real-Time Figyelő Indítása (Mentés ide: {RAW_FOLDER}) ---")
print("Megállításhoz nyomj CTRL+C-t!")

def get_realtime_data():
    try:
        response = requests.get(URL, timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            # Időbélyeg a fájlnévhez
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"bkk_100e_{timestamp}.json"
            filepath = os.path.join(RAW_FOLDER, filename)
            
            # Mentés fájlba
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            
            # Kiírjuk, hány buszt találtunk éppen
            bus_count = len(data.get('data', {}).get('list', []))
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Mentve: {filename} ({bus_count} db jármű a vonalon)")
        else:
            print(f"HIBA: A szerver {response.status_code} kóddal válaszolt.")
            
    except Exception as e:
        print(f"Hálózati HIBA: {e}")

# --- FŐCIKLUS ---
# 30 másodpercenként fut
try:
    while True:
        get_realtime_data()
        time.sleep(30) # Várakozás
except KeyboardInterrupt:
    print("\nLeállítás...")