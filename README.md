## Introduction 

In this project I am creating an ETL pipline to extract data Songs Data from S3 and processes it using Spark, and loads the data back into a new S3 bucket as a set of fact and dimensional tables. This will allow analytics team to continue finding insights in what songs users are listening to.

## Datasets Used
1) Contains Songs information
2) Contains Log information about the users

## Data Model

![alt text](https://github.com/surbhithole/spark_data_lake/blob/main/sparkify_erd.png)

## How to run the ETL Process:

1) Update AWS credentials in dl.cfg
2) >> python etl.py

#### Files in the repository:
1) etl.py --> Python file to execute the data pipeline
2) dl.cfg --> Update the AWS credentials in this file
