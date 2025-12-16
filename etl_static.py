import os
import pandas as pd
import sqlalchemy
import urllib
from datetime import date, timedelta

# =========================================================
# KONFIG
# =========================================================
DATA_FOLDER = r"C:\Users\SinkoGraphy\ME\School\uzleti_intelligencia_hf\BudAirportBI\Data"

SERVER = r"(localdb)\mssqllocaldb"
DATABASE = "BudAirportBI"
DRIVER = "ODBC Driver 17 for SQL Server"

OPENFLIGHTS_AIRPORTS = "airports.dat"
OPENFLIGHTS_ROUTES   = "routes.dat"
OPENFLIGHTS_AIRLINES = "airlines.dat"

GTFS_STOPS      = "stops.txt"
GTFS_ROUTES     = "routes.txt"
GTFS_TRIPS      = "trips.txt"
GTFS_STOP_TIMES = "stop_times.txt"
GTFS_CALDATES   = "calendar_dates.txt"

TARGET_SHORTNAME = "100E"        # ezt keressük a routes.route_short_name-ban (fallback route_desc)
STOP_TIMES_READ_CHUNK = 200_000  # stop_times chunk olvasás
TO_SQL_CHUNK = 10_000            # SQL batch size

print("=== BudAirportBI - ETL (GTFS header alapján + 100E-only stop_times + PK fix) ===")

# =========================================================
# DB kapcsolat
# =========================================================
connection_string = f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
params = urllib.parse.quote_plus(connection_string)
engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

def run_stmt(conn, sql: str):
    conn.execute(sqlalchemy.text(sql))

def p(filename: str) -> str:
    return os.path.join(DATA_FOLDER, filename)

# =========================================================
# Betöltők
# =========================================================
def load_openflights_dat(filename, table, schema, usecols, col_names):
    """OpenFlights .dat fájlokhoz (nincs header)"""
    file_path = p(filename)
    if not os.path.exists(file_path):
        print(f"⚠️ HIÁNYZIK: {file_path}")
        return

    print(f"→ Betöltés: {filename} -> {schema}.{table}")
    df = pd.read_csv(
        file_path,
        header=None,
        names=col_names,
        usecols=usecols,
        dtype=str,
        encoding="utf-8",
        on_bad_lines="skip",
    )
    df = df.replace({r"\N": None, "": None})
    df.to_sql(table, con=engine, schema=schema, if_exists="append", index=False, chunksize=TO_SQL_CHUNK)
    print(f"   ✅ Kész: {len(df)} sor.")

def load_gtfs_header_csv(filename, table, schema, usecols):
    """GTFS .txt fájlokhoz (header van) - FONTOS: header alapján választunk oszlopot"""
    file_path = p(filename)
    if not os.path.exists(file_path):
        print(f"⚠️ HIÁNYZIK: {file_path}")
        return

    print(f"→ Betöltés: {filename} -> {schema}.{table}")
    df = pd.read_csv(
        file_path,
        header=0,
        usecols=usecols,
        dtype=str,
        encoding="utf-8",
        on_bad_lines="skip",
    )
    df = df.replace({r"\N": None, "": None})
    df.to_sql(table, con=engine, schema=schema, if_exists="append", index=False, chunksize=TO_SQL_CHUNK)
    print(f"   ✅ Kész: {len(df)} sor.")

def load_stop_times_100e_only(trip_set):
    """stop_times csak 100E trip_id-kre szűrve, chunkolva"""
    file_path = p(GTFS_STOP_TIMES)
    if not os.path.exists(file_path):
        raise SystemExit(f"❌ HIÁNYZIK: {file_path}")

    total = 0
    for chunk in pd.read_csv(
        file_path,
        header=0,
        usecols=["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
        dtype=str,
        encoding="utf-8",
        on_bad_lines="skip",
        chunksize=STOP_TIMES_READ_CHUNK,
    ):
        chunk = chunk.replace({r"\N": None, "": None})
        chunk = chunk[chunk["trip_id"].isin(trip_set)]
        if not chunk.empty:
            chunk.to_sql("GTFS_StopTimes", con=engine, schema="stg", if_exists="append",
                         index=False, chunksize=TO_SQL_CHUNK)
            total += len(chunk)
            print(f"   ... +{len(chunk)} sor (100E stop_times összesen: {total})")

    print(f"✅ 100E stop_times kész. Betöltött sorok: {total}")
    return total

def generate_dim_date_fallback():
    """Dim_Date: fixen nagy intervallum (beadásbiztos), hogy biztos legyen mindenre DateKey"""
    start = date(2024, 1, 1)
    end   = date(2026, 12, 31)
    print(f"→ Dim_Date generálás: {start} .. {end}")

    rows = []
    curr = start
    while curr <= end:
        rows.append({
            "DateKey": curr.year * 10000 + curr.month * 100 + curr.day,
            "FullDate": curr,
            "DayName": curr.strftime("%A"),
            "IsWeekend": 1 if curr.weekday() >= 5 else 0
        })
        curr += timedelta(days=1)

    pd.DataFrame(rows).to_sql("Dim_Date", con=engine, schema="dw",
                              if_exists="append", index=False, chunksize=TO_SQL_CHUNK)
    print(f"   ✅ Dim_Date feltöltve: {len(rows)} nap.")


# =========================================================
# 0) SETUP: táblák (ha hiányoznak)
# =========================================================
with engine.begin() as conn:
    print("\n--- 0. SETUP ---")
    run_stmt(conn, "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='stg') EXEC('CREATE SCHEMA stg');")
    run_stmt(conn, "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='dw')  EXEC('CREATE SCHEMA dw');")

    # STG
    run_stmt(conn, """
    IF OBJECT_ID('stg.OpenFlights_Airports','U') IS NULL
    CREATE TABLE stg.OpenFlights_Airports (
        AirportID INT, Name NVARCHAR(255), City NVARCHAR(255), Country NVARCHAR(255),
        IATA VARCHAR(3), ICAO VARCHAR(4), Lat FLOAT, Lon FLOAT
    );""")
    run_stmt(conn, """
    IF OBJECT_ID('stg.OpenFlights_Routes','U') IS NULL
    CREATE TABLE stg.OpenFlights_Routes (
        Airline VARCHAR(3), AirlineID NVARCHAR(10),
        SourceAirport VARCHAR(3), SourceID NVARCHAR(10),
        DestAirport VARCHAR(3), DestID NVARCHAR(10)
    );""")
    run_stmt(conn, """
    IF OBJECT_ID('stg.OpenFlights_Airlines','U') IS NULL
    CREATE TABLE stg.OpenFlights_Airlines (
        AirlineID INT, Name NVARCHAR(255), Alias NVARCHAR(255), IATA VARCHAR(10), ICAO VARCHAR(10),
        Callsign NVARCHAR(100), Country NVARCHAR(100), Active VARCHAR(1)
    );""")

    run_stmt(conn, """
    IF OBJECT_ID('stg.GTFS_Stops','U') IS NULL
    CREATE TABLE stg.GTFS_Stops (
        stop_id VARCHAR(50), stop_name NVARCHAR(255), stop_lat FLOAT, stop_lon FLOAT
    );""")
    run_stmt(conn, """
    IF OBJECT_ID('stg.GTFS_Routes','U') IS NULL
    CREATE TABLE stg.GTFS_Routes (
        route_id VARCHAR(80), route_short_name VARCHAR(50), route_desc NVARCHAR(255)
    );""")
    run_stmt(conn, """
    IF OBJECT_ID('stg.GTFS_Trips','U') IS NULL
    CREATE TABLE stg.GTFS_Trips (
        route_id VARCHAR(80), service_id VARCHAR(50), trip_id VARCHAR(100) NOT NULL,
        trip_headsign NVARCHAR(255), direction_id VARCHAR(10), shape_id VARCHAR(80)
    );""")
    run_stmt(conn, """
    IF OBJECT_ID('stg.GTFS_StopTimes','U') IS NULL
    CREATE TABLE stg.GTFS_StopTimes (
        trip_id VARCHAR(100) NOT NULL,
        arrival_time VARCHAR(20),
        departure_time VARCHAR(20),
        stop_id VARCHAR(50) NOT NULL,
        stop_sequence VARCHAR(10)
    );""")
    run_stmt(conn, """
    IF OBJECT_ID('stg.GTFS_CalendarDates','U') IS NULL
    CREATE TABLE stg.GTFS_CalendarDates (
        service_id VARCHAR(50) NOT NULL,
        [date] VARCHAR(8) NOT NULL,
        exception_type VARCHAR(2) NOT NULL
    );""")

    # DW
    run_stmt(conn, """
    IF OBJECT_ID('dw.Dim_Airport','U') IS NULL
    CREATE TABLE dw.Dim_Airport (
        AirportID INT PRIMARY KEY, Name NVARCHAR(255), City NVARCHAR(255), Country NVARCHAR(255), IATA VARCHAR(3)
    );""")
    run_stmt(conn, """
    IF OBJECT_ID('dw.Dim_Stop','U') IS NULL
    CREATE TABLE dw.Dim_Stop (
        StopID VARCHAR(50) PRIMARY KEY, StopName NVARCHAR(255)
    );""")
    run_stmt(conn, """
    IF OBJECT_ID('dw.Dim_Airline','U') IS NULL
    CREATE TABLE dw.Dim_Airline (
        AirlineID INT PRIMARY KEY, Name NVARCHAR(255), IATA VARCHAR(10), Country NVARCHAR(100)
    );""")
    run_stmt(conn, """
    IF OBJECT_ID('dw.Dim_Date','U') IS NULL
    CREATE TABLE dw.Dim_Date (
        DateKey INT PRIMARY KEY, FullDate DATE, DayName NVARCHAR(20), IsWeekend BIT
    );""")
    run_stmt(conn, """
    IF OBJECT_ID('dw.Fact_FlightRoutes','U') IS NULL
    CREATE TABLE dw.Fact_FlightRoutes (
        RouteID INT IDENTITY(1,1) PRIMARY KEY,
        SourceAirportID INT,
        DestAirportID INT,
        AirlineID INT
    );""")

    run_stmt(conn, """
    IF OBJECT_ID('dw.Dim_RouteLine','U') IS NULL
    CREATE TABLE dw.Dim_RouteLine (
        RouteID VARCHAR(80) PRIMARY KEY,
        RouteShortName VARCHAR(50) NULL,
        RouteDesc NVARCHAR(255) NULL
    );""")

    run_stmt(conn, """
    IF OBJECT_ID('dw.Bridge_ServiceDate','U') IS NULL
    CREATE TABLE dw.Bridge_ServiceDate (
        ServiceID VARCHAR(50) NOT NULL,
        DateKey INT NOT NULL,
        IsActive BIT NOT NULL,
        CONSTRAINT PK_Bridge_ServiceDate PRIMARY KEY (ServiceID, DateKey)
    );""")

    run_stmt(conn, """
    IF OBJECT_ID('dw.Fact_ScheduledSegments','U') IS NULL
    CREATE TABLE dw.Fact_ScheduledSegments (
        SegmentID INT IDENTITY(1,1) PRIMARY KEY,
        RouteID VARCHAR(80) NOT NULL,
        ServiceID VARCHAR(50) NULL,
        TripID VARCHAR(100) NOT NULL,
        FromStopID VARCHAR(50) NOT NULL,
        ToStopID VARCHAR(50) NOT NULL,
        FromDepTimeSec INT NULL,
        ToArrTimeSec INT NULL,
        ScheduledDurSec INT NULL
    );""")

print("✅ Setup kész.")


# =========================================================
# 1) ÜRÍTÉS (újratöltés)
# =========================================================
with engine.begin() as conn:
    print("\n--- 1. ÜRÍTÉS ---")
    run_stmt(conn, "TRUNCATE TABLE stg.OpenFlights_Airports;")
    run_stmt(conn, "TRUNCATE TABLE stg.OpenFlights_Routes;")
    run_stmt(conn, "TRUNCATE TABLE stg.OpenFlights_Airlines;")
    run_stmt(conn, "TRUNCATE TABLE stg.GTFS_Stops;")
    run_stmt(conn, "TRUNCATE TABLE stg.GTFS_Routes;")
    run_stmt(conn, "TRUNCATE TABLE stg.GTFS_Trips;")
    run_stmt(conn, "TRUNCATE TABLE stg.GTFS_StopTimes;")
    run_stmt(conn, "TRUNCATE TABLE stg.GTFS_CalendarDates;")

    run_stmt(conn, "DELETE FROM dw.Fact_ScheduledSegments;")
    run_stmt(conn, "DELETE FROM dw.Bridge_ServiceDate;")
    run_stmt(conn, "DELETE FROM dw.Fact_FlightRoutes;")
    run_stmt(conn, "DELETE FROM dw.Dim_RouteLine;")
    run_stmt(conn, "DELETE FROM dw.Dim_Airport;")
    run_stmt(conn, "DELETE FROM dw.Dim_Airline;")
    run_stmt(conn, "DELETE FROM dw.Dim_Stop;")
    run_stmt(conn, "DELETE FROM dw.Dim_Date;")

print("✅ Ürítés kész.")


# =========================================================
# 2) STAGING betöltés (GTFS header alapján)
# =========================================================
print("\n--- 2. STAGING betöltés ---")

# OpenFlights
load_openflights_dat(
    OPENFLIGHTS_AIRPORTS, "OpenFlights_Airports", "stg",
    usecols=[0,1,2,3,4,5,6,7],
    col_names=["AirportID","Name","City","Country","IATA","ICAO","Lat","Lon"]
)
load_openflights_dat(
    OPENFLIGHTS_ROUTES, "OpenFlights_Routes", "stg",
    usecols=[0,1,2,3,4,5],
    col_names=["Airline","AirlineID","SourceAirport","SourceID","DestAirport","DestID"]
)
# airlines.dat-nál csak ezeket töltjük (a tábla többi oszlopa NULL marad)
load_openflights_dat(
    OPENFLIGHTS_AIRLINES, "OpenFlights_Airlines", "stg",
    usecols=[0,1,3,4,6],
    col_names=["AirlineID","Name","IATA","ICAO","Country"]
)

# GTFS (header!)
load_gtfs_header_csv(GTFS_STOPS,  "GTFS_Stops",  "stg", usecols=["stop_id","stop_name","stop_lat","stop_lon"])
load_gtfs_header_csv(GTFS_ROUTES, "GTFS_Routes", "stg", usecols=["route_id","route_short_name","route_desc"])
load_gtfs_header_csv(GTFS_TRIPS,  "GTFS_Trips",  "stg", usecols=["route_id","service_id","trip_id","trip_headsign","direction_id","shape_id"])
load_gtfs_header_csv(GTFS_CALDATES,"GTFS_CalendarDates","stg", usecols=["service_id","date","exception_type"])

print("✅ STAGING alap betöltés kész (stop_times még hátra).")


# =========================================================
# 2/B) 100E route_id-k -> trip_id-k -> csak azokra stop_times
# =========================================================
print("\n--- 2/B. CSAK 100E stop_times betöltés ---")

# Paraméterezés helyett FIX string: nincs többé :public hiba
routes_100e = pd.read_sql(
    f"""
    SELECT DISTINCT route_id, route_short_name, route_desc
    FROM stg.GTFS_Routes
    WHERE LTRIM(RTRIM(route_short_name)) = '{TARGET_SHORTNAME}'
       OR LTRIM(RTRIM(route_desc)) = '{TARGET_SHORTNAME}'
       OR route_desc LIKE '%{TARGET_SHORTNAME}%'
    """,
    engine
)

if routes_100e.empty:
    # Gyors fallback: ha mégsem található így, akkor inkább ne álljunk meg -> töltsünk mindent (lassabb, de beadásbiztos)
    print("⚠️ Nem találtam 100E route-ot a routes táblában (route_short_name/route_desc).")
    print("⚠️ Fallback: FULL stop_times betöltés (lassabb, de biztosan tovább megy).")

    # FULL stop_times betöltés chunkolva
    total = 0
    for chunk in pd.read_csv(
        p(GTFS_STOP_TIMES),
        header=0,
        usecols=["trip_id","arrival_time","departure_time","stop_id","stop_sequence"],
        dtype=str,
        encoding="utf-8",
        on_bad_lines="skip",
        chunksize=STOP_TIMES_READ_CHUNK
    ):
        chunk = chunk.replace({r"\N": None, "": None})
        chunk.to_sql("GTFS_StopTimes", con=engine, schema="stg", if_exists="append",
                     index=False, chunksize=TO_SQL_CHUNK)
        total += len(chunk)
        print(f"   ... +{len(chunk)} sor (FULL stop_times összesen: {total})")
    print(f"✅ FULL stop_times kész: {total} sor.")
else:
    route_ids = routes_100e["route_id"].astype(str).str.strip().dropna().unique().tolist()
    print("100E route_id(k):", route_ids[:10])

    # trip_id-k az adott route_id-khez
    # (IN lista stringgel: gyors, nincs paraméter marker gond)
    route_ids_sql = ",".join("'" + rid.replace("'", "''") + "'" for rid in route_ids)
    trip_ids_df = pd.read_sql(
        f"SELECT DISTINCT trip_id FROM stg.GTFS_Trips WHERE route_id IN ({route_ids_sql})",
        engine
    )

    trip_set = set(trip_ids_df["trip_id"].astype(str).tolist())
    print(f"✅ 100E trip_id-k száma: {len(trip_set)}")

    if len(trip_set) == 0:
        print("⚠️ 0 trip_id jött vissza a 100E-hez -> Fallback: FULL stop_times betöltés (beadásbiztos).")
        total = 0
        for chunk in pd.read_csv(
            p(GTFS_STOP_TIMES),
            header=0,
            usecols=["trip_id","arrival_time","departure_time","stop_id","stop_sequence"],
            dtype=str,
            encoding="utf-8",
            on_bad_lines="skip",
            chunksize=STOP_TIMES_READ_CHUNK
        ):
            chunk = chunk.replace({r"\N": None, "": None})
            chunk.to_sql("GTFS_StopTimes", con=engine, schema="stg", if_exists="append",
                         index=False, chunksize=TO_SQL_CHUNK)
            total += len(chunk)
            print(f"   ... +{len(chunk)} sor (FULL stop_times összesen: {total})")
        print(f"✅ FULL stop_times kész: {total} sor.")
    else:
        load_stop_times_100e_only(trip_set)


# =========================================================
# 3) Dim_Date
# =========================================================
print("\n--- 3. Dim_Date ---")
generate_dim_date_fallback()


# =========================================================
# 4) DW feltöltés
# =========================================================
with engine.begin() as conn:
    print("\n--- 4. DW feltöltés ---")

    print("→ dw.Dim_Airport")
    run_stmt(conn, """
        INSERT INTO dw.Dim_Airport (AirportID, Name, City, Country, IATA)
        SELECT DISTINCT CAST(AirportID AS INT), Name, City, Country, IATA
        FROM stg.OpenFlights_Airports
        WHERE AirportID IS NOT NULL AND ISNUMERIC(AirportID)=1;
    """)

    print("→ dw.Dim_Stop")
    run_stmt(conn, """
        INSERT INTO dw.Dim_Stop (StopID, StopName)
        SELECT DISTINCT stop_id, stop_name
        FROM stg.GTFS_Stops
        WHERE stop_id IS NOT NULL;
    """)

    print("→ dw.Dim_Airline")
    run_stmt(conn, """
        INSERT INTO dw.Dim_Airline (AirlineID, Name, IATA, Country)
        SELECT DISTINCT CAST(AirlineID AS INT), Name, IATA, Country
        FROM stg.OpenFlights_Airlines
        WHERE AirlineID IS NOT NULL AND ISNUMERIC(AirlineID)=1;
    """)

    # ✅ PK-duplikáció elleni védelem: 1 sor / route_id
    print("→ dw.Dim_RouteLine (PK FIX: deduplikálás)")
    run_stmt(conn, """
        WITH x AS (
            SELECT
                LTRIM(RTRIM(route_id)) AS RouteID,
                route_short_name AS RouteShortName,
                route_desc AS RouteDesc,
                ROW_NUMBER() OVER (
                    PARTITION BY LTRIM(RTRIM(route_id))
                    ORDER BY
                      CASE WHEN route_short_name IS NOT NULL AND LTRIM(RTRIM(route_short_name))<>'' THEN 0 ELSE 1 END,
                      route_short_name,
                      route_desc
                ) AS rn
            FROM stg.GTFS_Routes
            WHERE route_id IS NOT NULL AND LTRIM(RTRIM(route_id)) <> ''
        )
        INSERT INTO dw.Dim_RouteLine (RouteID, RouteShortName, RouteDesc)
        SELECT RouteID, RouteShortName, RouteDesc
        FROM x
        WHERE rn = 1;
    """)

    print("→ dw.Fact_FlightRoutes (BUD szűréssel)")
    run_stmt(conn, """
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
          AND ISNUMERIC(a.AirlineID)=1;
    """)

    print("→ dw.Bridge_ServiceDate")
    run_stmt(conn, """
        INSERT INTO dw.Bridge_ServiceDate (ServiceID, DateKey, IsActive)
        SELECT
            cd.service_id,
            d.DateKey,
            CASE WHEN cd.exception_type = '1' THEN 1 ELSE 0 END
        FROM stg.GTFS_CalendarDates cd
        JOIN dw.Dim_Date d
          ON d.DateKey = TRY_CAST(cd.[date] AS INT);
    """)

    # Menetrendi szakaszok: ha stop_times 100E-only, akkor ez is az lesz
    print("→ dw.Fact_ScheduledSegments")
    run_stmt(conn, f"""
        WITH base AS (
            SELECT
                t.route_id AS RouteID,
                t.service_id AS ServiceID,
                st.trip_id AS TripID,
                st.stop_id AS FromStopID,
                LEAD(st.stop_id) OVER (PARTITION BY st.trip_id ORDER BY TRY_CAST(st.stop_sequence AS INT)) AS ToStopID,
                st.departure_time AS FromDepTime,
                LEAD(st.arrival_time) OVER (PARTITION BY st.trip_id ORDER BY TRY_CAST(st.stop_sequence AS INT)) AS ToArrTime
            FROM stg.GTFS_StopTimes st
            JOIN stg.GTFS_Trips t ON t.trip_id = st.trip_id
            WHERE TRY_CAST(st.stop_sequence AS INT) IS NOT NULL
        )
        INSERT INTO dw.Fact_ScheduledSegments
        (RouteID, ServiceID, TripID, FromStopID, ToStopID, FromDepTimeSec, ToArrTimeSec, ScheduledDurSec)
        SELECT
            RouteID,
            ServiceID,
            TripID,
            FromStopID,
            ToStopID,

            (TRY_CAST(PARSENAME(REPLACE(FromDepTime,':','.'),3) AS INT) * 3600
             + TRY_CAST(PARSENAME(REPLACE(FromDepTime,':','.'),2) AS INT) * 60
             + TRY_CAST(PARSENAME(REPLACE(FromDepTime,':','.'),1) AS INT)
            ) AS FromDepTimeSec,

            (TRY_CAST(PARSENAME(REPLACE(ToArrTime,':','.'),3) AS INT) * 3600
             + TRY_CAST(PARSENAME(REPLACE(ToArrTime,':','.'),2) AS INT) * 60
             + TRY_CAST(PARSENAME(REPLACE(ToArrTime,':','.'),1) AS INT)
            ) AS ToArrTimeSec,

            CASE
              WHEN
                (TRY_CAST(PARSENAME(REPLACE(ToArrTime,':','.'),3) AS INT) * 3600
                 + TRY_CAST(PARSENAME(REPLACE(ToArrTime,':','.'),2) AS INT) * 60
                 + TRY_CAST(PARSENAME(REPLACE(ToArrTime,':','.'),1) AS INT))
                <
                (TRY_CAST(PARSENAME(REPLACE(FromDepTime,':','.'),3) AS INT) * 3600
                 + TRY_CAST(PARSENAME(REPLACE(FromDepTime,':','.'),2) AS INT) * 60
                 + TRY_CAST(PARSENAME(REPLACE(FromDepTime,':','.'),1) AS INT))
              THEN
                (TRY_CAST(PARSENAME(REPLACE(ToArrTime,':','.'),3) AS INT) * 3600
                 + TRY_CAST(PARSENAME(REPLACE(ToArrTime,':','.'),2) AS INT) * 60
                 + TRY_CAST(PARSENAME(REPLACE(ToArrTime,':','.'),1) AS INT)) + 86400
                -
                (TRY_CAST(PARSENAME(REPLACE(FromDepTime,':','.'),3) AS INT) * 3600
                 + TRY_CAST(PARSENAME(REPLACE(FromDepTime,':','.'),2) AS INT) * 60
                 + TRY_CAST(PARSENAME(REPLACE(FromDepTime,':','.'),1) AS INT))
              ELSE
                (TRY_CAST(PARSENAME(REPLACE(ToArrTime,':','.'),3) AS INT) * 3600
                 + TRY_CAST(PARSENAME(REPLACE(ToArrTime,':','.'),2) AS INT) * 60
                 + TRY_CAST(PARSENAME(REPLACE(ToArrTime,':','.'),1) AS INT))
                -
                (TRY_CAST(PARSENAME(REPLACE(FromDepTime,':','.'),3) AS INT) * 3600
                 + TRY_CAST(PARSENAME(REPLACE(FromDepTime,':','.'),2) AS INT) * 60
                 + TRY_CAST(PARSENAME(REPLACE(FromDepTime,':','.'),1) AS INT))
            END AS ScheduledDurSec
        FROM base
        WHERE ToStopID IS NOT NULL
          AND FromDepTime IS NOT NULL
          AND ToArrTime IS NOT NULL;
    """)

print("✅ DW feltöltés kész.")


# =========================================================
# 5) Ellenőrzések
# =========================================================
with engine.connect() as conn:
    print("\n--- 5. Ellenőrzések ---")
    checks = [
        ("stg.GTFS_StopTimes", "SELECT COUNT(*) FROM stg.GTFS_StopTimes"),
        ("dw.Dim_RouteLine", "SELECT COUNT(*) FROM dw.Dim_RouteLine"),
        ("dw.Fact_ScheduledSegments", "SELECT COUNT(*) FROM dw.Fact_ScheduledSegments"),
        ("dw.Bridge_ServiceDate", "SELECT COUNT(*) FROM dw.Bridge_ServiceDate"),
    ]
    for name, sql in checks:
        c = conn.execute(sqlalchemy.text(sql)).scalar()
        print(f"{name}: {c}")

print("\n=== KÉSZ. Power BI: Refresh ===")
