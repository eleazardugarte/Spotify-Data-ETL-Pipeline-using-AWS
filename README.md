# Spotify-Data-ETL-Pipeline-using-AWS
Implemented an ETL (Extract, Transform, Load) pipeline using Python and AWS services to extract data from the Spotify API.

## Overview

### 1) Extraction
- Developed a Python script to extract data from the Spotify API.
- Deployed the script on AWS Lambda, with daily triggers managed by Amazon CloudWatch.
- Raw data was stored in Amazon S3 for further processing.

### 2) Transformation
- Implemented another AWS Lambda function in Python to transform the raw data into a structured format.
- The transformed data was saved in a separate S3 bucket for organized storage.

### 3) Loading and Data Exposure
- Utilized AWS Glue Crawlers to infer the schema of the transformed data.
- Populated the AWS Glue Data Catalog, making the data accessible for querying via Amazon Athena.
- Enabled real-time analytics by exposing the data for further analysis.

## Tools Used
- **Programming Language:** Python
- **Amazon Web Services:** AWS Lambda, Amazon CloudWatch, Amazon S3, AWS Glue, Amazon Athena
- **Dataset:** Spotify API
