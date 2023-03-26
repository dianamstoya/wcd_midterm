# AWS EMR Data Processing Pipeline

This repository contains code for an AWS EMR data processing pipeline that performs ETL on data stored in S3 using PySpark and Airflow. The pipeline is triggered by an AWS Lambda function when new data is added to the input bucket.

## Files

The repository contains the following files:

`lambda_function.py`: This is the AWS Lambda function that is triggered by S3 when files arrive in the input bucket. It checks whether all required files are present and triggers an Airflow DAG if they are. If not, it sends an email notification. The send_email.py file is used for sending the email.

`midterm_dag.py`: This file contains the code for the Airflow DAG that creates an EMR cluster for the subsequent processing using Spark and monitors the execution.

`etl_pyspark_script.py`: This is the PySpark script that is executed on the EMR cluster for ETL processing. The cluster terminates upon successful execution.

`Midterm.pbix`: This is a Power BI report file to visualize the final data out of Amazon Athena.

## Installation

To install and set up the data processing pipeline, follow these steps:

- Clone the repository: `git clone https://github.com/dianamstoya/wcd_midterm.git`
- Set up your AWS credentials and configure your environment to work with AWS.
- Create an S3 bucket for input data.
- Create an S3 bucket for output data.
- Edit the lambda_function.py file to include the names of your input and output S3 buckets.
- Upload the lambda_function.py file to AWS Lambda and set it up to trigger on S3 events.
- Edit the midterm_dag.py file to include the necessary configurations for your EMR cluster and PySpark script.
- Upload the midterm_dag.py file to your Airflow instance
- Set up a Glue Crawler to crawl the output bucket and create tables
- Use Athena to access the tables (e.g. using Power BI)

## Usage

To use the data processing pipeline, simply add new data files to the input S3 bucket. The Lambda function will trigger the Airflow DAG to process the data on the EMR cluster. The final processed data will be stored in the output S3 bucket, crawled by Glue and the final tables can be visualized using the Power BI report file.
