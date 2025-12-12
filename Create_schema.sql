USE master;

GO

-- 2. Új adatbázis létrehozása
CREATE DATABASE BudAirportBI;
GO

USE BudAirportBI;
GO

-- 3. Sémák
CREATE SCHEMA stg; -- Nyers adatoknak
GO
CREATE SCHEMA dw;  -- Kész riportoknak (Csillagséma)
GO

-- 4. STAGING Táblák (Ide tölt majd a Python)
CREATE TABLE stg.OpenFlights_Airports (
    AirportID INT, Name NVARCHAR(255), City NVARCHAR(255), Country NVARCHAR(255), 
    IATA VARCHAR(3), ICAO VARCHAR(4), Lat FLOAT, Lon FLOAT
);

CREATE TABLE stg.OpenFlights_Routes (
    Airline VARCHAR(3), AirlineID NVARCHAR(10), 
    SourceAirport VARCHAR(3), SourceID NVARCHAR(10), 
    DestAirport VARCHAR(3), DestID NVARCHAR(10)
);

-- ÚJ: Légitársaságok nyers tábla
CREATE TABLE stg.OpenFlights_Airlines (
    AirlineID INT, Name NVARCHAR(255), Alias NVARCHAR(255), IATA VARCHAR(10), ICAO VARCHAR(10), 
    Callsign NVARCHAR(100), Country NVARCHAR(100), Active VARCHAR(1)
);

CREATE TABLE stg.GTFS_Stops (
    stop_id VARCHAR(50), stop_name NVARCHAR(255), 
    stop_lat FLOAT, stop_lon FLOAT
);

CREATE TABLE stg.GTFS_Routes (
    route_id VARCHAR(50), route_short_name VARCHAR(50), route_desc NVARCHAR(255)
);

-- 5. ADATTÁRHÁZ Táblák (DW)
CREATE TABLE dw.Dim_Airport (
    AirportID INT PRIMARY KEY, Name NVARCHAR(255), City NVARCHAR(255), Country NVARCHAR(255), IATA VARCHAR(3)
);

CREATE TABLE dw.Dim_Stop (
    StopID VARCHAR(50) PRIMARY KEY, StopName NVARCHAR(255)
);

-- ÚJ: Légitársaság Dimenzió
CREATE TABLE dw.Dim_Airline (
    AirlineID INT PRIMARY KEY,
    Name NVARCHAR(255),
    IATA VARCHAR(10),
    Country NVARCHAR(100)
);

CREATE TABLE dw.Dim_Date (
    DateKey INT PRIMARY KEY, FullDate DATE, DayName NVARCHAR(20), IsWeekend BIT
);

-- MÓDOSÍTOTT TÉNYTÁBLA: AirlineID (szám) köti össze a név helyett!
CREATE TABLE dw.Fact_FlightRoutes (
    RouteID INT IDENTITY(1,1) PRIMARY KEY,
    SourceAirportID INT, 
    DestAirportID INT, 
    AirlineID INT -- Foreign Key a dw.Dim_Airline táblához
);
GO