INSERT INTO `show create default.raw_data` (`statement`) VALUES
	 ('CREATE TABLE default.raw_data
(
    `user_id` String,
    `event_time` DateTime,
    `event_id` String
)
ENGINE = MergeTree
ORDER BY user_id
SETTINGS index_granularity = 8192');
