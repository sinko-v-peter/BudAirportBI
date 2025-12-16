USE BudAirportBI;
GO

CREATE OR ALTER PROCEDURE dw.usp_BuildAgg_Transit_15m
    @DaysBack INT = 14
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @FromDT DATETIME2(0) = DATEADD(DAY, -@DaysBack, CAST(GETDATE() AS DATE));
    DECLARE @FromDateKey INT = CONVERT(INT, CONVERT(VARCHAR(8), CAST(@FromDT AS DATE), 112));

    -- egyszerû és beadás-biztos újraszámolás: törlés + újratöltés az érintett idõablakra
    DELETE FROM dw.Agg_Transit_15m
    WHERE DateKey >= @FromDateKey;

    ;WITH x AS (
        -- delay statisztikákhoz
        SELECT
            CONVERT(INT, CONVERT(VARCHAR(8), CAST(SnapshotDT AS DATE), 112)) AS DateKey,
            (DATEPART(HOUR, SnapshotDT)*60 + DATEPART(MINUTE, SnapshotDT))/15 AS TimeSlot,
            StopId,
            RouteIdRT,
            DelaySec
        FROM stg.RealTime_StopArrivals
        WHERE SnapshotDT >= @FromDT
          AND DelaySec IS NOT NULL
    ),
    stats AS (
        SELECT
            DateKey, TimeSlot, StopId, RouteIdRT,
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
    hw_raw AS (
        -- headway soronként: LEAD kiszámolása külön szinten
        SELECT
            CONVERT(INT, CONVERT(VARCHAR(8), CAST(SnapshotDT AS DATE), 112)) AS DateKey,
            (DATEPART(HOUR, SnapshotDT)*60 + DATEPART(MINUTE, SnapshotDT))/15 AS TimeSlot,
            StopId,
            RouteIdRT,
            DATEDIFF(
                SECOND,
                PredictedArrivalDT,
                LEAD(PredictedArrivalDT) OVER (
                    PARTITION BY SnapshotDT, StopId, RouteIdRT
                    ORDER BY PredictedArrivalDT
                )
            ) AS HeadwaySec
        FROM stg.RealTime_StopArrivals
        WHERE SnapshotDT >= @FromDT
          AND PredictedArrivalDT IS NOT NULL
    ),
    hw AS (
        -- itt már lehet átlagolni
        SELECT
            DateKey, TimeSlot, StopId, RouteIdRT,
            AVG(CAST(HeadwaySec AS FLOAT)) AS AvgHeadwaySec
        FROM hw_raw
        WHERE HeadwaySec IS NOT NULL
          AND HeadwaySec > 0
          AND HeadwaySec < 7200 -- 2 óra felett gyanús, dobd
        GROUP BY DateKey, TimeSlot, StopId, RouteIdRT
    )
    INSERT INTO dw.Agg_Transit_15m
        (DateKey, TimeSlot, StopId, RouteIdRT, AvgDelaySec, P95DelaySec, OnTimeRatio, ObsCount, AvgHeadwaySec)
    SELECT
        s.DateKey,
        s.TimeSlot,
        s.StopId,
        s.RouteIdRT,
        s.AvgDelaySec,
        p.P95DelaySec,
        s.OnTimeRatio,
        s.ObsCount,
        h.AvgHeadwaySec
    FROM stats s
    JOIN p95 p
      ON p.DateKey = s.DateKey AND p.TimeSlot = s.TimeSlot AND p.StopId = s.StopId AND p.RouteIdRT = s.RouteIdRT
    LEFT JOIN hw h
      ON h.DateKey = s.DateKey AND h.TimeSlot = s.TimeSlot AND h.StopId = s.StopId AND h.RouteIdRT = s.RouteIdRT;
END
GO
