import pandas as pd
import sqlalchemy
import urllib
import os
from datetime import date, timedelta

# --- KONFIGURÁCIÓ ---
DATA_FOLDER = r"C:\Users\SinkoGraphy\ME\School\uzleti_intelligencia_hf\BudAirportBI\Data"
SERVER = r'(localdb)\mssqllocaldb' 
DATABASE = 'BudAirportBI'
DRIVER = 'ODBC Driver 17 for SQL Server'

print("--- BudAirport BI: ETL Indítása (Új Dimenzióval) ---")

# Kapcsolódás
connection_string = f'DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'
params = urllib.parse.quote_plus(connection_string)

try:
    engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)
    with engine.connect() as conn:
        print(f"✅ SIKERES KAPCSOLAT: {SERVER}")
except Exception as e:
    print(f"❌ KAPCSOLÓDÁSI HIBA: {e}")
    exit()

def load_table(filename, table, cols_keep, col_names, header=0):
    path = os.path.join(DATA_FOLDER, filename)
    print(f"Feldolgozás: {filename} -> {table}...")
    try:
        # dtype=str: Minden string, biztonsági okokból
        df = pd.read_csv(path, header=header, names=col_names, usecols=cols_keep, encoding='utf-8', on_bad_lines='skip', dtype=str)
        df = df.replace({'\\N': None, '': None})
        df.to_sql(table, con=engine, schema='stg', if_exists='append', index=False, chunksize=5000)
        print(f"   -> {len(df)} sor betöltve.")
    except Exception as e:
        print(f"   -> HIBA: {e} (Ellenőrizd, hogy a fájl ott van-e a mappában!)")

# 1. TÁBLÁK ÜRÍTÉSE (Csak a biztonság kedvéért, ha többször futtatnád)
print("\n--- 1. Fázis: Staging táblák ürítése ---")
with engine.connect() as conn:
    conn.execute(sqlalchemy.text("TRUNCATE TABLE stg.OpenFlights_Airports"))
    conn.execute(sqlalchemy.text("TRUNCATE TABLE stg.OpenFlights_Routes"))
    conn.execute(sqlalchemy.text("TRUNCATE TABLE stg.OpenFlights_Airlines"))
    conn.execute(sqlalchemy.text("TRUNCATE TABLE stg.GTFS_Stops"))
    conn.execute(sqlalchemy.text("TRUNCATE TABLE stg.GTFS_Routes"))
    # A DW táblákat is ürítjük a konzisztencia miatt
    conn.execute(sqlalchemy.text("DELETE FROM dw.Fact_FlightRoutes"))
    conn.execute(sqlalchemy.text("DELETE FROM dw.Dim_Airport"))
    conn.execute(sqlalchemy.text("DELETE FROM dw.Dim_Airline"))
    conn.execute(sqlalchemy.text("DELETE FROM dw.Dim_Stop"))
    conn.execute(sqlalchemy.text("DELETE FROM dw.Dim_Date"))
    conn.commit()

# 2. CSV BETÖLTÉS (STAGING)
print("\n--- 2. Fázis: Nyers adatok betöltése ---")
# Repterek
load_table('airports.dat', 'OpenFlights_Airports', [0,1,2,3,4,5,6,7], ['AirportID','Name','City','Country','IATA','ICAO','Lat','Lon'], None)
# Útvonalak
load_table('routes.dat', 'OpenFlights_Routes', [0,1,2,3,4,5], ['Airline','AirlineID','SourceAirport','SourceID','DestAirport','DestID'], None)
# Légitársaságok (ÚJ!)
load_table('airlines.dat', 'OpenFlights_Airlines', [0,1,3,4,6], ['AirlineID','Name','IATA','ICAO','Country'], None)
# GTFS
load_table('stops.txt', 'GTFS_Stops', ['stop_id','stop_name','stop_lat','stop_lon'], ['stop_id','stop_name','stop_lat','stop_lon'], 0)
load_table('routes.txt', 'GTFS_Routes', ['route_id','route_short_name','route_desc'], ['route_id','route_short_name','route_desc'], 0)


# 3. DÁTUM DIMENZIÓ (PYTHON GENERÁLÁS)
print("\n--- 3. Fázis: Dátum dimenzió generálása ---")
try:
    start_date = date(2024, 1, 1)
    end_date = date(2025, 12, 31)
    date_rows = []
    curr = start_date
    while curr <= end_date:
        date_rows.append({
            'DateKey': curr.year * 10000 + curr.month * 100 + curr.day,
            'FullDate': curr,
            'DayName': curr.strftime('%A'),
            'IsWeekend': 1 if curr.weekday() >= 5 else 0
        })
        curr += timedelta(days=1)

    df_dates = pd.DataFrame(date_rows)
    df_dates.to_sql('Dim_Date', con=engine, schema='dw', if_exists='append', index=False)
    print(f"✅ Dátumok feltöltve: {len(df_dates)} nap.")
except Exception as e:
    print(f"❌ HIBA a dátumoknál: {e}")


# 4. ADATTÁRHÁZ FELTÖLTÉSE (SQL LOGIKA)
print("\n--- 4. Fázis: Adattárház feltöltése (Csillagséma) ---")
with engine.connect() as conn:
    try:
        # A) Repterek
        print("   -> Dim_Airport...")
        conn.execute(sqlalchemy.text("""
            INSERT INTO dw.Dim_Airport (AirportID, Name, City, Country, IATA)
            SELECT DISTINCT CAST(AirportID AS INT), Name, City, Country, IATA 
            FROM stg.OpenFlights_Airports 
            WHERE AirportID IS NOT NULL AND ISNUMERIC(AirportID) = 1
        """))
        
        # B) Megállók
        print("   -> Dim_Stop...")
        conn.execute(sqlalchemy.text("""
            INSERT INTO dw.Dim_Stop (StopID, StopName)
            SELECT DISTINCT stop_id, stop_name FROM stg.GTFS_Stops
        """))
        
        # C) Légitársaságok (ÚJ!)
        print("   -> Dim_Airline...")
        conn.execute(sqlalchemy.text("""
            INSERT INTO dw.Dim_Airline (AirlineID, Name, IATA, Country)
            SELECT DISTINCT CAST(AirlineID AS INT), Name, IATA, Country
            FROM stg.OpenFlights_Airlines
            WHERE AirlineID IS NOT NULL AND ISNUMERIC(AirlineID) = 1
        """))
        
        # D) Járatok (Ténytábla)
        # Itt kötjük össze az útvonalat a légitársaság ID-jával
        print("   -> Fact_FlightRoutes (BUD szűréssel)...")
        conn.execute(sqlalchemy.text("""
            INSERT INTO dw.Fact_FlightRoutes (SourceAirportID, DestAirportID, AirlineID)
            SELECT 
                TRY_CAST(r.SourceID AS INT), 
                TRY_CAST(r.DestID AS INT), 
                CAST(a.AirlineID AS INT)
            FROM stg.OpenFlights_Routes r
            INNER JOIN stg.OpenFlights_Airlines a 
                ON (r.Airline = a.IATA OR r.Airline = a.ICAO)
            WHERE (r.SourceAirport = 'BUD' OR r.DestAirport = 'BUD')
              AND ISNUMERIC(r.SourceID)=1 AND ISNUMERIC(r.DestID)=1
        """))
        
        conn.commit()
        print("✅ Adattárház (DW) táblák sikeresen feltöltve!")
        
    except Exception as e:
        print(f"❌ Hiba a DW töltésekor: {e}")

print("\n--- KÉSZ! Mehetsz a Power BI-ba! ---")