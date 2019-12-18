# us-accidents-data-enginering

### Datasets
- [US Accidents](https://www.kaggle.com/sobhanmoosavi/us-accidents) (2.25 million records, 819 MB, CSV)
- [US Cities: Demographics](https://public.opendatasoft.com/explore/dataset/us-cities-demographics) (2892 records, 255 KB, CSV)
- [Airport Codes](https://datahub.io/core/airport-codes#data) (17 MB, JSON)

## Getting started

#### Datalake schema
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

#### Analyze datalake with Athena
[AWS blogpost](https://aws.amazon.com/blogs/machine-learning/run-sql-queries-from-your-sagemaker-notebooks-using-amazon-athena/)
```
ipython kernel install --name "my-venv" --user
```

#### Create python virtual environment
```
python3 -m venv venv             # create virtualenv
source venv/bin/activate         # activate virtualenv
pip install -r requirements.txt  # install requirements
```

#### Extract the dataset
```
# install the 7z package
sudo apt-get install p7zip-full

# decompress data files
7z x ./dataset/data.7z
```

#### Split files into multiple smaller files
```
cd src/
python -m script.split_data
```

#### Start Airflow Container
```
docker-compose.yml
```

#### Connect Airflow to AWS

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