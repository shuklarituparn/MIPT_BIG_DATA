USE shuklari;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.exec.max.dynamic.partitions=2000;
SET hive.exec.max.dynamic.partitions.pernode=1000;
DROP TABLE IF EXISTS user_logs;
DROP TABLE IF EXISTS Logs;
DROP TABLE IF EXISTS Users;
DROP TABLE IF EXISTS IPRegions;
DROP TABLE IF EXISTS Subnets;

CREATE EXTERNAL TABLE user_logs (
    IpAddr STRING,
    Timestamp STRING,
    HttpRequest STRING,
    Response SMALLINT,
    StatusCode SMALLINT,
    ClientInformation STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '^(\\S+)\\t{3}(\\d+)\\t(\\S+)\\t(\\d+)\\t(\\d+)\\t([^\\s]+).*$'
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_logs_M';


CREATE EXTERNAL TABLE Logs (
    IpAddr STRING,
    HttpRequest STRING,
    Response SMALLINT,
    StatusCode SMALLINT,
    Browser STRING,
    FullTimeStamp STRING
)
PARTITIONED BY (Date STRING)
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE Logs PARTITION (Date)
SELECT
    IpAddr,
    HttpRequest,
    Response,
    StatusCode,
    split(ClientInformation, ' ')[0] AS Browser,
    Timestamp AS FullTimeStamp,
    substr(Timestamp, 1, 8) AS Date
FROM user_logs;

CREATE EXTERNAL TABLE Users (
    ip STRING,
    browser STRING,
    gender STRING,
    age TINYINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_data_M/';

CREATE EXTERNAL TABLE IPRegions (
    ip STRING,
    region STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/user_logs/ip_data_M';


CREATE EXTERNAL TABLE Subnets (
    ip STRING,
    mask STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/subnets/variant2';

SELECT * FROM user_logs LIMIT 10;
SELECT * FROM Logs LIMIT 10;
SELECT * FROM Users  LIMIT 10;
SELECT * FROM IPRegions  LIMIT 10;
SELECT * FROM Subnets  LIMIT 10;