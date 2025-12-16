import os, time, json, urllib, requests
from datetime import datetime
import pandas as pd
import sqlalchemy

# ========= ÁLLÍTSD BE =========
STOP_ID_RT = "BKK_F00950"  # <-- IDE írd be a FUTÁR stopId-t (BKK_Fxxxxx)
ROUTE_ID_RT = "BKK_1005"   # 100E routeId a FUTÁR-ban
POLL_SEC = 30

DATA_FOLDER = r"C:\Users\SinkoGraphy\ME\School\uzleti_intelligencia_hf\BudAirportBI\Data\arrivals"
RAW_FOLDER = os.path.join(DATA_FOLDER, "RealTime_JSON")
os.makedirs(RAW_FOLDER, exist_ok=True)

SERVER = r"(localdb)\mssqllocaldb"
DATABASE = "BudAirportBI"
DRIVER = "ODBC Driver 17 for SQL Server"

BASE = "https://futar.bkk.hu/api/query/v1/ws/otp/api/where"
KEY = "bkk-web"
APPV = "1.1.abc"

ARR_URL = (
    f"{BASE}/arrivals-and-departures-for-stop.json"
    f"?stopId={STOP_ID_RT}&minutesBefore=0&minutesAfter=60&key={KEY}&appVersion={APPV}"
)

# ========= DB =========
connection_string = f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
params = urllib.parse.quote_plus(connection_string)
engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

def epoch_to_dt(x):
    if x is None:
        return None
    x = int(x)
    if x > 10**12:
        x //= 1000
    return datetime.fromtimestamp(x)

def ensure_tables():
    # Minimál: ha nincs, hozza létre
    ddl1 = """
    IF OBJECT_ID('stg.RealTime_StopArrivals','U') IS NULL
    CREATE TABLE stg.RealTime_StopArrivals(
        SnapshotDT DATETIME2(0) NOT NULL,
        StopId VARCHAR(50) NULL,
        RouteIdRT VARCHAR(50) NULL,
        TripId VARCHAR(100) NULL,
        ScheduledArrivalDT DATETIME2(0) NULL,
        PredictedArrivalDT DATETIME2(0) NULL,
        DelaySec INT NULL,
        RawFile NVARCHAR(260) NULL
    );
    """
    ddl2 = """
    IF OBJECT_ID('stg.RealTime_StopHeadway','U') IS NULL
    CREATE TABLE stg.RealTime_StopHeadway(
        SnapshotDT DATETIME2(0) NOT NULL,
        StopId VARCHAR(50) NULL,
        RouteIdRT VARCHAR(50) NULL,
        HeadwaySec INT NULL,
        RawFile NVARCHAR(260) NULL
    );
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(ddl1)
        conn.exec_driver_sql(ddl2)

def fetch_json():
    r = requests.get(ARR_URL, timeout=20)
    r.raise_for_status()
    return r.json()

def normalize_route(x: str) -> str:
    # biztos ami biztos: "BKK_1005" vs "1005"
    if not x:
        return ""
    return x.replace(" ", "")

def main():
    ensure_tables()
    print(f"--- Real-time ARRIVALS (Stop={STOP_ID_RT}, Route={ROUTE_ID_RT}, poll={POLL_SEC}s) ---")
    print(f"Mentés ide: {RAW_FOLDER}")
    print("Leállítás: CTRL+C")

    while True:
        snap_dt = datetime.now().replace(microsecond=0)
        ts = snap_dt.strftime("%Y%m%d_%H%M%S")
        raw_name = f"bkk_100e_arr_{ts}.json"
        raw_path = os.path.join(RAW_FOLDER, raw_name)

        try:
            data = fetch_json()
        except Exception as e:
            print(f"[{snap_dt.strftime('%H:%M:%S')}] HIBA: {e}")
            time.sleep(POLL_SEC)
            continue

        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        entry = (data.get("data") or {}).get("entry") or {}
        refs  = (data.get("data") or {}).get("references") or {}
        trips_ref = refs.get("trips") or {}

        rows = []
        pred_times = []

        aad = entry.get("arrivalsAndDepartures")
        if isinstance(aad, list) and len(aad) > 0:
            # --- ESET 1: arrivalsAndDepartures (ha valamikor ilyen jön) ---
            for a in aad:
                rid = a.get("routeId")
                if rid != "BKK_1005":
                    continue
                
                sched = epoch_to_dt(a.get("scheduledArrivalTime")) or epoch_to_dt(a.get("scheduledDepartureTime"))
                pred  = epoch_to_dt(a.get("predictedArrivalTime")) or epoch_to_dt(a.get("predictedDepartureTime"))

                delay = None
                if sched and pred:
                    delay = int((pred - sched).total_seconds())

                if pred:
                    pred_times.append(pred)

                rows.append({
                    "SnapshotDT": snap_dt,
                    "StopId": entry.get("stopId"),
                    "RouteIdRT": rid,
                    "TripId": a.get("tripId"),
                    "ScheduledArrivalDT": sched,
                    "PredictedArrivalDT": pred,
                    "DelaySec": delay,
                    "RawFile": raw_name
                })

        else:
            # --- ESET 2: stopTimes (NÁLAD EZ JÖN) ---
            st = entry.get("stopTimes") or []
            for s in st:
                trip_id = s.get("tripId")
                rid = (trips_ref.get(trip_id) or {}).get("routeId")  # <- innen jön, és a fájlban BKK_1005 :contentReference[oaicite:3]{index=3}
                if rid != "BKK_1005":
                    continue
                
                # stopTimes-ban ezek a mezők vannak: departureTime / predictedDepartureTime (epoch sec)
                sched = epoch_to_dt(s.get("departureTime"))  # scheduled
                pred  = epoch_to_dt(s.get("predictedDepartureTime")) or sched  # ha nincs predicted, legyen sched

                delay = None
                if sched and pred:
                    delay = int((pred - sched).total_seconds())

                if pred:
                    pred_times.append(pred)

                rows.append({
                    "SnapshotDT": snap_dt,
                    "StopId": entry.get("stopId"),
                    "RouteIdRT": rid,
                    "TripId": trip_id,
                    "ScheduledArrivalDT": sched,
                    "PredictedArrivalDT": pred,
                    "DelaySec": delay,
                    "RawFile": raw_name
                })


                # headway: a következő predikciók közti különbség
                head_rows = []
                pred_times = sorted([t for t in pred_times if t is not None])
                for i in range(1, len(pred_times)):
                    hw = int((pred_times[i] - pred_times[i-1]).total_seconds())
                    # szűrés: 1 perc .. 60 perc között
                    if 60 <= hw <= 3600:
                        head_rows.append({
                            "SnapshotDT": snap_dt,
                            "StopId": STOP_ID_RT,
                            "RouteIdRT": ROUTE_ID_RT,
                            "HeadwaySec": hw,
                            "RawFile": raw_name
                        })

        if rows:
            pd.DataFrame(rows).to_sql("RealTime_StopArrivals", con=engine, schema="stg", if_exists="append", index=False)
        if head_rows:
            pd.DataFrame(head_rows).to_sql("RealTime_StopHeadway", con=engine, schema="stg", if_exists="append", index=False)

        print(f"[{snap_dt.strftime('%H:%M:%S')}] 100E rows={len(rows)} | headways={len(head_rows)} | file={raw_name}")
        time.sleep(POLL_SEC)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nLeállítás...")
