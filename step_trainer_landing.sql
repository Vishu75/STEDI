CREATE EXTERNAL TABLE IF NOT EXISTS stedi.step_trainer_landing (
    sensorReadingTime BIGINT,
    serialNumber STRING,
    distanceFromObject INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    "ignore.malformed.json" = "FALSE",
    "case.insensitive" = "TRUE",
    "dots.in.keys" = "FALSE"
)
STORED AS
    INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    's3://vs-stedi-human-balance-analytics/step_trainer/landing/'
TBLPROPERTIES (
    "classification" = "json"
);
