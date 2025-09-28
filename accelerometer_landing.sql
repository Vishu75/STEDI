CREATE DATABASE IF NOT EXISTS stedi;
CREATE EXTERNAL TABLE stedi.accelerometer_landing
(
   user_id    STRING,
   event_time STRING,
   accel_x    DOUBLE,
   accel_y    DOUBLE,
   accel_z    DOUBLE
)
STORED AS TEXTFILE
LOCATION 's3://vs-stedi-human-balance-analytics/accelerometer/landing/'
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
   "ignore.malformed.json" = "false",
   "case.insensitive"      = "true"
)
TBLPROPERTIES (
   "classification" = "json"
);

