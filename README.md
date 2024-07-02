# RedditDataPipeline
# Data Pipeline with Reddit, Airflow, Celery, Postgres, S3, AWS Glue, Athena, and Redshift

This project provides a comprehensive data pipeline solution to extract, transform, and load (ETL) Reddit data into a Redshift data warehouse. The pipeline leverages a combination of tools and services including Apache Airflow, Celery, PostgreSQL, Amazon S3, AWS Glue, Amazon Athena, and Amazon Redshift.


## Overview
The pipeline is designed to:
- **Extract** data from Reddit using its API.
- **Store** the raw data into an S3 bucket from Airflow.
- **Transform** the data using AWS Glue and Amazon Athena.
- **Load** the transformed data into Amazon Redshift for analytics and querying.

## Used Services
- **Reddit API**: Source of the data.
- **Apache Airflow & Celery**: Orchestrates the ETL process and manages task distribution.
- **PostgreSQL**: Temporary storage and metadata management.
- **Amazon S3**: Raw data storage.
- **AWS Glue**: Data cataloging and ETL jobs.
- **Amazon Athena**: SQL-based data transformation.
- **Amazon Redshift**: Data warehousing and analytics.


# T20WC 2024 Data Analysis