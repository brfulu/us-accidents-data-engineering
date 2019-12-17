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
    df = df.filter(df.continent == 'NA')
    print('airport_coint = ', df.count())

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

    # read city data files
    df = spark.read.csv(city_data, header=True, sep=';')
    df = df.drop_duplicates(subset=['City', 'State'])
    print('city_count = ', df.count())

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

    print('accident_count = ', df.count())

    # read in song data to use for city table
    city_df = spark.read.parquet(os.path.join(output_data, 'cities/state=*/*.parquet'))
    print('city_count = ', city_df.count())

    # read in song data to use for airport table
    airport_df = spark.read.parquet(os.path.join(output_data, 'airports/state=*/*.parquet'))
    print('airport_count = ', airport_df.count())

    joined_df = df.join(city_df, (df.City == city_df.city_name) & (df.State == city_df.state_code), how='left')
    print('joined1_count = ', joined_df.count())
    joined_df = joined_df.join(airport_df, 'airport_code', how='left')
    print('joined2_count = ', joined_df.count())
    # joined_df.show(5)

    joined_df = joined_df.withColumn('datetime', F.to_date(F.col('Start_Time')))

    # extract columns to create accidents table
    accident_table = joined_df.select(
        F.col('ID').alias('accident_id'),
        F.year('datetime').alias('year'),
        F.month('datetime').alias('month'),
        # F.dayofmonth('datetime').alias('day'),
        'datetime',
        F.col('City').alias('city'),
        F.col('State').alias('state_code'),
        F.col('State').alias('state'),
        F.col('Airport_Code').alias('airport_code'),
        F.col('Start_Time').alias('start_time'),
        F.col('End_Time').alias('end_time'),
        F.col('Timezone').alias('timezone'),
        # F.col('Description').alias('description'),
        F.col('Severity').alias('severity'),
        F.col('Temperature(F)').alias('temperature')
    )

    # accident_table.show(5)
    accident_table.write.partitionBy(['year', 'month']).parquet(os.path.join(output_data, 'accidents'), 'overwrite')


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
