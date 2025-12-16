USE BudAirportBI;
GO

/* 1) DEMO view: a valós StopArrivals-ból számol egy mesterséges DelaySec-et
   - csak akkor ad "kamu" késést, ha a valós DelaySec 0 vagy NULL
   - determinisztikus (Snapshot percébõl), tehát reprodukálható */
CREATE OR ALTER VIEW dw.vw_StopArrivals_Demo AS
WITH base AS (
    SELECT
        SnapshotDT,
        StopId,
        RouteIdRT,
        TripId,
        ScheduledArrivalDT,
        COALESCE(PredictedArrivalDT, ScheduledArrivalDT) AS PredBase,
        COALESCE(DelaySec, 0) AS DelayBase,
        RawFile
    FROM stg.RealTime_StopArrivals
),
demo AS (
    SELECT
        *,
        CASE
            WHEN DelayBase <> 0 THEN DelayBase
            ELSE
                CASE (DATEPART(MINUTE, SnapshotDT) % 10)
                    WHEN 0 THEN  60   -- +1 perc
                    WHEN 1 THEN 180   -- +3 perc
                    WHEN 2 THEN 300   -- +5 perc
                    WHEN 3 THEN 420   -- +7 perc
                    WHEN 4 THEN 600   -- +10 perc
                    WHEN 5 THEN -60   -- -1 perc (korábban)
                    ELSE 0
                END
        END AS DelaySec_Demo
    FROM base
)
SELECT
    SnapshotDT,
    StopId,
    RouteIdRT,
    TripId,
    ScheduledArrivalDT,
    DATEADD(SECOND, DelaySec_Demo, ScheduledArrivalDT) AS PredictedArrivalDT,
    DelaySec_Demo AS DelaySec,
    RawFile
FROM demo;
GO

/* 2) DEMO agg tábla */
IF OBJECT_ID('dw.Agg_Transit_15m_Demo','U') IS NULL
CREATE TABLE dw.Agg_Transit_15m_Demo (
    DateKey         INT NOT NULL,
    TimeSlot        INT NOT NULL,
    StopId          VARCHAR(50) NOT NULL,
    RouteIdRT       VARCHAR(50) NOT NULL,
    AvgDelaySec     FLOAT NULL,
    P95DelaySec     FLOAT NULL,
    OnTimeRatio     FLOAT NULL,
    ObsCount        INT NOT NULL,
    AvgHeadwaySec   FLOAT NULL,
    CONSTRAINT PK_Agg_Transit_15m_Demo PRIMARY KEY (DateKey, TimeSlot, StopId, RouteIdRT)
);
GO

/* 3) DEMO aggregáló SP: a demo view-ból számol Delay KPI-t, a headway-t a valós headway táblából */
CREATE OR ALTER PROCEDURE dw.usp_BuildAgg_Transit_15m_Demo
    @DaysBack INT = 7
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @FromDT DATETIME2(0) = DATEADD(DAY, -@DaysBack, CAST(GETDATE() AS DATE));
    DECLARE @FromDateKey INT = CONVERT(INT, CONVERT(VARCHAR(8), CAST(@FromDT AS DATE), 112));

    DELETE FROM dw.Agg_Transit_15m_Demo
    WHERE DateKey >= @FromDateKey;

    ;WITH x AS (
        SELECT
            CONVERT(INT, CONVERT(VARCHAR(8), CAST(SnapshotDT AS DATE), 112)) AS DateKey,
            (DATEPART(HOUR, SnapshotDT)*60 + DATEPART(MINUTE, SnapshotDT))/15 AS TimeSlot,
            StopId,
            RouteIdRT,
            DelaySec
        FROM dw.vw_StopArrivals_Demo
        WHERE SnapshotDT >= @FromDT
          AND DelaySec IS NOT NULL
    ),
    stats AS (
        SELECT DateKey, TimeSlot, StopId, RouteIdRT,
               AVG(CAST(DelaySec AS FLOAT)) AS AvgDelaySec,
               AVG(CASE WHEN ABS(DelaySec) <= 60 THEN 1.0 ELSE 0.0 END) AS OnTimeRatio,
               COUNT(*) AS ObsCount
        FROM x
        GROUP BY DateKey, TimeSlot, StopId, RouteIdRT
    ),
    p95 AS (
        SELECT DISTINCT
            DateKey, TimeSlot, StopId, RouteIdRT,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY CAST(DelaySec AS FLOAT))
              OVER (PARTITION BY DateKey, TimeSlot, StopId, RouteIdRT) AS P95DelaySec
        FROM x
    ),
    hw AS (
        SELECT
            CONVERT(INT, CONVERT(VARCHAR(8), CAST(SnapshotDT AS DATE), 112)) AS DateKey,
            (DATEPART(HOUR, SnapshotDT)*60 + DATEPART(MINUTE, SnapshotDT))/15 AS TimeSlot,
            StopId,
            RouteIdRT,
            AVG(CAST(HeadwaySec AS FLOAT)) AS AvgHeadwaySec
        FROM stg.RealTime_StopHeadway
        WHERE SnapshotDT >= @FromDT
          AND HeadwaySec IS NOT NULL
        GROUP BY
            CONVERT(INT, CONVERT(VARCHAR(8), CAST(SnapshotDT AS DATE), 112)),
            (DATEPART(HOUR, SnapshotDT)*60 + DATEPART(MINUTE, SnapshotDT))/15,
            StopId, RouteIdRT
    )
    INSERT INTO dw.Agg_Transit_15m_Demo (DateKey, TimeSlot, StopId, RouteIdRT, AvgDelaySec, P95DelaySec, OnTimeRatio, ObsCount, AvgHeadwaySec)
    SELECT s.DateKey, s.TimeSlot, s.StopId, s.RouteIdRT,
           s.AvgDelaySec, p.P95DelaySec, s.OnTimeRatio, s.ObsCount,
           h.AvgHeadwaySec
    FROM stats s
    JOIN p95 p
      ON p.DateKey=s.DateKey AND p.TimeSlot=s.TimeSlot AND p.StopId=s.StopId AND p.RouteIdRT=s.RouteIdRT
    LEFT JOIN hw h
      ON h.DateKey=s.DateKey AND h.TimeSlot=s.TimeSlot AND h.StopId=s.StopId AND h.RouteIdRT=s.RouteIdRT;
END
GO
