counts = '''
select count(*) from clickhouse.channel_timeseries
'''

year = """

WITH metadata AS
(
    SELECT
        channel_uuid,
        channel_name AS channel,
        channel_type
    FROM clickhouse.channel_metadata FINAL
    WHERE location_id = '69243da80da1c071834ac6e4'
      AND channel_type IN ('battery', 'load')
      AND channel_name IS NOT NULL
      AND channel_name != ''
      AND (__deleted = 'false' OR __deleted IS NULL)
)
SELECT
    toStartOfInterval(ts.timestamp_utc, INTERVAL 5 MINUTE) AS timestamp_utc,
    m.channel,
    sum(
        multiIf(
            m.channel_type = 'battery' AND ts.power < 0, abs(ts.power),
            m.channel_type = 'load', ts.power,
            0
        )
    ) AS power
FROM clickhouse.channel_timeseries AS ts
INNER JOIN metadata AS m
    ON ts.channel_uuid = m.channel_uuid
                                                                                                                                                            
WHERE ts.timestamp_utc >= '2025-01-28 00:00:00'
  AND ts.timestamp_utc <= '2026-02-20 23:59:59'
  AND (ts.__deleted = 'false' OR ts.__deleted IS NULL)
GROUP BY
    timestamp_utc,
    m.channel
ORDER BY
    timestamp_utc,
    m.channel;

"""