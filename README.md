# DW-on-AWS-Redshift
# Introduction
The goal of this project is to build an ETL pipeline for a database hosted on Redshift. 
The ETL pipeline extracts the data from S3, stages it in Redshift, and transforms the data into a set of fact and dimension tables for the BI applications to consume.

![](images/img1.png)

# Project description
Mock music app Company, Sparkify's user base and song database has grown significantly and therefore want to move their data onto the Cloud. 
This data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

# Data description
There are two main datasets that reside in S3.

 * Song data: s3://udacity-dend/song-data/
 * Log data: s3://udacity-dend/log-data/
 * The log format is captured in s3://udacity-dend/log_json_path.json

# Song dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. 

Log dataset
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.


# Technolgy Used
* Amazon Redshift
* Python

# Star Schema
![](images/img2.png)

# Project Design
* create_cluster.py : This script creates the redshift cluster on AWS.
* create_tables.py: Script for creating the staging and star-schema tables.
* etl.py: Script for loading data into staging tables and then inserting data into star-schema.
* sql_queries.py: Script contains all the queries used by create_tables.py and etl.py.
* dwh-iac.cfg: Configuration file for creating RedShift cluster, used by create_cluster.py.
* dwh.cfg: Configuration file used by create_tables.py and etl.py,create_cluster.py,delete_cluster.py
* delete_cluster.py: This script is used to delete the cluster.

# Project Execution
* Step 1: Create the Redshift Cluster using create_cluster.py
* Step 2: Execute the create_tables.py to create the staging and schema tables on Redshift
* Step 3: Execute the etl.py to load the data in Redshift.


