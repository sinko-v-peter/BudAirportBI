USE BudAirportBI;
GO

SELECT 'Repterek (DW)' AS Tabla, COUNT(*) AS Darab FROM dw.Dim_Airport
UNION ALL
SELECT 'Megállók (DW)', COUNT(*) FROM dw.Dim_Stop
UNION ALL
SELECT 'Légijáratok (DW)', COUNT(*) FROM dw.Fact_FlightRoutes
UNION ALL
SELECT 'Dátumok (DW)', COUNT(*) FROM dw.Dim_Date;