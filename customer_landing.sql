CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customers_landing (
    customerName              STRING,
    email                     STRING,
    phone                     STRING,
    birthDay                  STRING,
    serialNumber              STRING,
    registrationDate          BIGINT,
    lastUpdateDate            BIGINT,
    shareWithResearchAsOfDate BIGINT,
    shareWithPublicAsOfDate   BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'case.insensitive' = 'true',
    'dots.in.keys'     = 'false',
    'ignore.malformed.json' = 'false'
)
STORED AS 
    INPUTFORMAT  'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://vs-stedi-human-balance-analytics/customer/landing/'
TBLPROPERTIES (
    'classification' = 'json'
);
