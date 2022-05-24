INSERT INTO `show create default.calculated_data` (`statement`) VALUES
	 ('CREATE VIEW default.calculated_data
(
    `data` DateTime,
    `user_id` String,
    `session_id` UInt32,
    `session_duration` Int32
) AS
WITH a AS
    (
        SELECT
            event_time AS data,
            user_id,
            event_id,
            multiIf((event_time - neighbor(event_time, -1)) > 900, ''start'', (neighbor(event_time, 1) - event_time) > 900, ''stop'', NULL) AS act_start
        FROM
        (
            SELECT *
            FROM default.raw_data
            ORDER BY
                user_id ASC,
                event_id ASC,
                event_time ASC
        )
        WHERE (act_start = ''start'') OR (act_start = ''stop'')
    )
SELECT
    data,
    user_id,
    rand(1) AS session_id,
    stat_data - data AS session_duration
FROM
(
    SELECT
        *,
        neighbor(act_start, 1) AS stat,
        neighbor(data, 1) AS stat_data
    FROM a
)
WHERE (stat = ''stop'') AND (session_duration > 0)');
