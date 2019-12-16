# us-accidents-data-enginering

### Datasets
- [US Accidents](https://www.kaggle.com/sobhanmoosavi/us-accidents) (2.25 million records, 819 MB, CSV)
- [US Cities: Demographics](https://public.opendatasoft.com/explore/dataset/us-cities-demographics) (2892 records, 255 KB, CSV)
- [Airport Codes](https://datahub.io/core/airport-codes#data) (17 MB, JSON)

## Getting started

### Create python virtual environment
```
python3 -m venv venv             # create virtualenv
source venv/bin/activate         # activate virtualenv
pip install -r requirements.txt  # install requirements
```

### Extract the dataset
```
# install the 7z package
sudo apt-get install p7zip-full

# decompress data files
7z x ./dataset/data.7z
```

### Split files into multiple smaller files
```
cd src/
python -m script.split_data
```
