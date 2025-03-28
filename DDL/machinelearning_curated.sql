{\rtf1\ansi\ansicpg1252\cocoartf2639
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx566\tx1133\tx1700\tx2267\tx2834\tx3401\tx3968\tx4535\tx5102\tx5669\tx6236\tx6803\pardirnatural\partightenfactor0

\f0\fs24 \cf0 CREATE EXTERNAL TABLE IF NOT EXISTS `uda-datalake-db`.`machinelearning_curated` (\
  `sensorreadingtime` bigint,\
  `serialnumber` string,\
  `distancefromobject` int,\
  `timestamp` bigint,\
  `email` string,\
  `x` int,\
  `y` int,\
  `z` int\
)\
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'\
WITH SERDEPROPERTIES (\
  'ignore.malformed.json' = 'FALSE',\
  'dots.in.keys' = 'FALSE',\
  'case.insensitive' = 'TRUE',\
  'mapping' = 'TRUE'\
)\
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\
LOCATION 's3://uda-spark-datalake/step-trainer/curated/'\
TBLPROPERTIES ('classification' = 'json');}