CREATE EXTERNAL TABLE IF NOT EXISTS `nasaccelerometerprojectdb`.`accelerometer_landing` (
  `user` string,
  `timeStamp` bigint,
  `x` float,
  `y` float,
  `z` float
)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'case.insensitive'='TRUE', 
  'dots.in.keys'='FALSE', 
  'ignore.malformed.json'='FALSE', 
  'mapping'='TRUE') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://nasaccelerometerproject/sources/accelerometer/'
TBLPROPERTIES (
  'classification'='json'
)
