DROP TABLE IF EXISTS #StagingEvents;
SELECT IDENTITY( INT, 1, 1) AS ID, 
       PARSE([event_date] AS    DATE USING 'AR-LB') AS [event_date], 
       convert(INT,[event_id]  ) AS [event_id], 
       convert(INT,[user_id]  ) AS [user_id]
INTO #StagingEvents
FROM [SparkNetworks].[dbo].[Staging_Events];


CREATE CLUSTERED INDEX PK_StagingEvents
ON #StagingEvents(ID ASC)

DROP TABLE IF EXISTS #StagingUsers;
SELECT convert(INT,[user_id] ) AS [user_id], 
       SUBSTRING(email, CHARINDEX('@', email) + 1, LEN(email)) provider_domain
INTO #StagingUsers
FROM [SparkNetworks].[dbo].[Staging_Users];

CREATE CLUSTERED INDEX PK_StagingUsers
ON #StagingUsers([user_id] ASC)

--ALTER Table StagingEvents
--ALTER COLUMN [user_id] ADD MASKED WITH (FUNCTION= 'random(1,9999999)') 

;WITH cte
     AS (SELECT DATEPART(wk, evn.[event_date]) week_number, 
                evn.user_id AS anonymized_user_id, 
                ISNULL(provider_domain, 'xxx-undefined') provider_domain, 
                evn.event_id
         FROM #StagingEvents evn
              LEFT JOIN #StagingUsers users ON evn.user_id = users.user_id),
     per_week
     AS (SELECT week_number, 
                COUNT(DISTINCT event_id) count_per_week
         FROM cte
         GROUP BY week_number),
     per_user_week
     AS (SELECT week_number, 
                anonymized_user_id, 
                COUNT(DISTINCT event_id) count_per_user_week
         FROM cte
         GROUP BY week_number, 
                  anonymized_user_id),
     per_week_domain
     AS (SELECT week_number, 
                provider_domain, 
                COUNT(DISTINCT event_id) count_per_week_domain
         FROM cte
         GROUP BY week_number, 
                  provider_domain),
     per_user_week_domain
     AS (SELECT week_number, 
                anonymized_user_id, 
                provider_domain, 
                COUNT(DISTINCT event_id) count_per_user_week_domain
         FROM cte
         GROUP BY week_number, 
                  anonymized_user_id, 
                  provider_domain),
     rate_per_provider
     AS (SELECT pw.week_number, 
                anonymized_user_id, 
                pw.provider_domain, 
                CAST(CAST(count_per_user_week_domain AS DECIMAL(21, 1)) / CAST(count_per_week_domain AS DECIMAL(21, 1)) AS DECIMAL(21, 2)) AS provider_event_rate
         FROM per_week_domain pw
              INNER JOIN per_user_week_domain puw ON pw.week_number = puw.week_number
                                                     AND pw.provider_domain = puw.provider_domain)
     SELECT rpp.week_number, 
            rpp.anonymized_user_id, 
            rpp.provider_domain, 
            rpp.provider_event_rate, 
            CAST(CAST(count_per_user_week AS DECIMAL(21, 1)) / CAST(count_per_week AS DECIMAL(21, 1)) AS DECIMAL(21, 2)) as overall_event_rate
     FROM rate_per_provider rpp
          INNER JOIN per_user_week puw ON rpp.week_number = puw.week_number
                                          AND rpp.anonymized_user_id = puw.anonymized_user_id
          INNER JOIN per_week pw ON pw.week_number = rpp.week_number;


		  SELECT  User_id,
        GroupingSet = DATEADD(DAY, 
                            -ROW_NUMBER() OVER(PARTITION BY User_id 
                                                        ORDER BY [event_date]), 
                            [event_date])

FROM    #StagingEvents;


;WITH 

  dates AS (
    SELECT DISTINCT User_id, CAST(event_date AS DATE) event_date
    FROM #StagingEvents
    
  ),

  groups AS (
    SELECT User_id,
      ROW_NUMBER() OVER (PARTITION BY User_id ORDER BY event_date) AS rn,
      dateadd(day, -ROW_NUMBER() OVER (PARTITION BY User_id ORDER BY event_date), event_date) AS grp,
      event_date
    FROM dates
  )
SELECT user_id,
  COUNT(*) AS consecutive_days,
  MIN(event_date) AS start_date,
  MAX(event_date) AS end_date
FROM groups
GROUP BY User_id,grp
having COUNT(*)>2