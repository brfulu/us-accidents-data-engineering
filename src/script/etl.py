import configparser
from datetime import datetime
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_airpot_data(spark, input_data, output_data):
    # get filepath to airport data file
    airport_data = os.path.join(input_data, 'airport_data/*.csv')

    # read airport data files
    df = spark.read.csv(airport_data, header=True)

    # extract columns to create songs table
    airport_table = df.select(
        F.col('ident').alias('airport_code'),
        F.col('iso_country').alias('state'),
        'type',
        'name',
        'continent',
        F.col('iso_country').alias('state_code'),
        'iso_region',
        'municipality'
    )

    airport_table.write.partitionBy('state').parquet(os.path.join(output_data, 'airports'), 'overwrite')


def process_city_data(spark, input_data, output_data):
    # get filepath to city data file
    city_data = os.path.join(input_data, 'city_data/*.csv')
    print(city_data)

    # read airport data files
    df = spark.read.csv(city_data, header=True, sep=';')

    # extract columns to create city table
    city_table = df.select(
        F.monotonically_increasing_id().alias('city_id'),
        F.col('City').alias('city_name'),
        F.col('State Code').alias('state'),
        F.col('State Code').alias('state_code'),
        F.col('Total Population').alias('total_population')
    )

    city_table.write.partitionBy('state').parquet(os.path.join(output_data, 'cities'), 'overwrite')


def process_accident_data(spark, input_data, output_data):
    # get filepath to accident data file
    accident_data = os.path.join(input_data, 'accident_data/*.csv')
    print(accident_data)

    # read accident data files
    df = spark.read.csv(accident_data, header=True)

    # extract relevant columns
    df = df['ID', 'Start_Time', 'City', 'State', 'Airport_Code', 'End_Time', 'Timezone', 'Description', 'Severity',
            'Temperature(F)']

    # read in song data to use for songplays table
    city_df = spark.read.parquet(os.path.join(output_data, 'cities/state=*/*.parquet'))
    print(city_df.count())

    city_accident_df = df.join(city_df, df.State == city_df.state_code)

    print(city_accident_df.count())

    city_accident_df.show(5)
    print('poceo')

    # extract columns to create accidents table
    accident_table = city_accident_df.select(
        F.col('ID').alias('accident_id'),
        F.col('Start_Time').alias('datetime'),
        F.col('City').alias('city'),
        F.col('State').alias('state_code'),
        F.col('State').alias('state'),
        F.col('Airport_Code').alias('airport_code'),
        F.col('Start_Time').alias('start_time'),
        F.col('End_Time').alias('end_time'),
        F.col('Timezone').alias('timezone'),
        F.col('Description').alias('description'),
        F.col('Severity').alias('severity'),
        F.col('Temperature(F)').alias('temperature')
    )

    accident_table.write.partitionBy(['datetime', 'state']).parquet(os.path.join(output_data, 'accidents'), 'overwrite')
    print('zavrsio')


def main():
    if len(sys.argv) == 3:
        # aws cluster mode
        input_data = sys.argv[1]
        output_data = sys.argv[2]
    else:
        # local mode
        config = configparser.ConfigParser()
        config.read('../dl.cfg')

        os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
        os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

        input_data = 's3a://' + config['S3']['RAW_DATALAKE_BUCKET'] + '/'
        output_data = 's3a://' + config['S3']['ACCIDENTS_DATALAKE_BUCKET'] + '/'

    spark = create_spark_session()

    process_airpot_data(spark, input_data, output_data)
    process_city_data(spark, input_data, output_data)
    process_accident_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
