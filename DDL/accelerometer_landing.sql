{\rtf1\ansi\ansicpg1252\cocoartf2639
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fnil\fcharset0 Monaco;}
{\colortbl;\red255\green255\blue255;\red13\green16\blue20;\red255\green255\blue255;}
{\*\expandedcolortbl;;\cssrgb\c5882\c7843\c10196;\cssrgb\c100000\c100000\c100000;}
\paperw11900\paperh16840\margl1440\margr1440\vieww25400\viewh15440\viewkind0
\deftab720
\pard\pardeftab720\partightenfactor0

\f0\fs24 \cf2 \cb3 \expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 CREATE EXTERNAL TABLE `accelerometer_landing`(\
  `user` string COMMENT 'from deserializer', \
  `timestamp` bigint COMMENT 'from deserializer', \
  `x` float COMMENT 'from deserializer', \
  `y` float COMMENT 'from deserializer', \
  `z` float COMMENT 'from deserializer')\
ROW FORMAT SERDE \
  'org.openx.data.jsonserde.JsonSerDe' \
STORED AS INPUTFORMAT \
  'org.apache.hadoop.mapred.TextInputFormat' \
OUTPUTFORMAT \
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\
LOCATION\
  's3://uda-spark-datalake/accelerometer/landing/'\
TBLPROPERTIES (\
  'classification'='json')\
}