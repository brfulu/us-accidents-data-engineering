# us-accidents-data-enginering

## Introduction

This is a capstone project for the Udacity DataEngineering Nanodegree.
The purpose of the data engineering capstone project is to give you a chance to combine everything learned throughout the program. 

#### Datasets
We are going to work with 3 different datasets and try to combine them in a useful way to extract meaningful information.

Dataset Sources:
- [US Accidents](https://www.kaggle.com/sobhanmoosavi/us-accidents) (2.25 million records, 819 MB, CSV)
    - This is a countrywide traffic accident dataset, which covers 49 states of the United States. 
- [US Cities: Demographics](https://public.opendatasoft.com/explore/dataset/us-cities-demographics) (2892 records, 255 KB, CSV)
    - This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. 
- [Airport Codes](https://datahub.io/core/airport-codes#data) (17 MB, JSON)
    - This dataset contains the list of many airport codes in the wordls.
    
#### Goal
The idea is to create an optimized data lake which will enable users to analyze accidents data and determine root causes of accidents.
The main goal of this project is to build an end to end data pipeline which is capable to work with big volumes of data.
We want to clean, transform and load the data to our optimized data lake on S3.
The data lake will consist of logical tables partitioned by certain columns to optimize query latency.
    
## Explore Data Quality
First we need to explore the data to identify data quality issues, like missing values, duplicate data, etc.


## Data Model
As mentioned in the introduction, the data will be modeled in a data lake on S3.
Data lakes are a relatively new concept, which was introduced during the increase of the volume of the data companies 
are working with every day. The idea is that we want to have a single source of truth for data in our system. At the 
beginning we may not be sure in how many different ways are we going to use this data so a flexible schema is needed.
Data is stored as object blobs or plain files, and usually partitioned in folders by some columns.

For this project we are going to construct 2 data lakes:
1. Raw Data Lake
    - The purpose of this data lake is to represent a single source of truth and to store all kinds of data generated 
    from different sources in raw format. This is the first step to move our data to the cloud. It is usually a good 
    idea to retain the raw data, because we can always go back to our raw data lake and change our ETL process or easily
    add new extraction pipelines.
    - Here we will store our 3 data sets partitioned in 3 folders. Each dataset is slit in multiple smaller csv files. 

2. Optimized Data Lake
    - This is what we are using for analytics. The data is prepared, compressed and paritioned by certain  columns to 
    allow for fast query times.
    - We are consturcting a star schema with 1 fact table and multiple dimension tables.
#### Optimized Datalake schema
Fact table
1. accidents
    - accident_id: This is a unique identifier of the accident record.
    - severity: Shows the severity of the accident, a number between 1 and 4
    - distance: The length of the road extent affected by the accident.
    - description: Shows natural language description of the accident.
    - airport_code
    - city_id
    - temperature: Shows the temperature (in Fahrenheit).

Dimension tables
1. cities
    - city_id
    - city
    - state_code
    - total_population

2. airports
    - airport_code
    - type
    - name
    - iso_country (state_code)
    - iso_region
    - municipality

## Getting started
Now we are going to follow steps from decompressing the original datasets to creating an optimized data lake and 
run queries against it using Amazon Athena and Apache Spark.

#### Project structure 
```
us-accidents-data-engineering
│   README.md                    # Project description
│   docker-compose.yml           # Airflow containers description   
│   requirements.txt             # Python dependencies
|   dag.png                      # Pipeline DAG image
│   
└───airflow                      # Airflow home
|   |               
│   └───dags                     # Jupyter notebooks
│   |   │ s3_to_redshift_dag.py  # DAG definition
|   |   |
|   └───plugins
│       │  
|       └───helpers
|       |   | sql_queries.py     # All sql queries needed
|       |
|       └───operators
|       |   | data_quality.py    # DataQualityOperator
|       |   | load_dimension.py  # LoadDimensionOperator
|       |   | load_fact.py       # LoadFactOperator
|       |   | stage_redshift.py  # StageToRedshiftOperator
```

#### Step 1: Clone repository to local machine
```
git clone https://github.com/brfulu/us-accidents-data-engineering.git
```

#### Step 2: Create python virtual environment
```
python3 -m venv venv             # create virtualenv
source venv/bin/activate         # activate virtualenv
pip install -r requirements.txt  # install requirements
```

#### Step 3: Extract the dataset
```
# install the 7z package
sudo apt-get install p7zip-full

# decompress data files
7z x ./dataset/data.7z
```

#### Step 4: Split files into multiple smaller files
```
cd src/
python -m script.split_data
```

#### Step 5: Start Airflow Container
```
docker-compose.yml
```

#### Step 6: Connect Airflow to AWS

1. Click on the Admin tab and select Connections.
![Admin tab](https://video.udacity-data.com/topher/2019/February/5c5aaca1_admin-connections/admin-connections.png)

2. Under Connections, select Create.

3. On the create connection page, enter the following values:
- Conn Id: Enter aws_credentials.
- Conn Type: Enter Amazon Web Services.
- Login: Enter your Access key ID from the IAM User credentials.
- Password: Enter your Secret access key from the IAM User credentials.
![aws_credentials](https://video.udacity-data.com/topher/2019/February/5c5aaefe_connection-aws-credentials/connection-aws-credentials.png)
Click save to confirm.

#### Step 7: Change default EMR config in Airflow
1. Click on the Admin tab and select Connections.
2. Select the 'emr_default' connection
3. Copy everything from `src/helper/emr_default.json` and paste into the field 'Extra'
4. Click save

#### Step 8: Start raw_datalake DAG
This pipeline creates the S3 bucket for our raw datalake and uploads the files from local machine.
Wait until the pipeline has successfully completed.

#### Step 9: Start optimized datalake ETL DAG
This pipeline extracts the data from our raw datalake, transforms is using Spark on an EMR cluster and saves it in 
way that is optimizing our query efficiency.

#### Step 10: Analyze datalake with Athena
Please refer to the following blogpost for mor detailed instructions.
[AWS blogpost](https://aws.amazon.com/blogs/machine-learning/run-sql-queries-from-your-sagemaker-notebooks-using-amazon-athena/)
```
ipython kernel install --name "my-venv" --user
```