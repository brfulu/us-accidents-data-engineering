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


def process_accident_data(spark, input_data, output_data):
    """Process accident data

    Extract transform and load the accident dataset into an optimized data lake on S3.

    :param spark: spark session object
    :param input_data: string; input_data path
    :param output_data: string; output_data path
    """

    # get filepath to accident data file
    accident_data = os.path.join(input_data, 'accident_data/*.csv')

    # read accident data files
    df = spark.read.csv(accident_data, header=True)

    # extract relevant columns
    df = df['ID', 'Start_Time', 'City', 'State', 'Airport_Code', 'End_Time', 'Timezone', 'Description', 'Severity',
            'Temperature(F)', 'Distance(mi)', 'Wind_Speed(mph)', 'Precipitation(in)',
            'Weather_Condition', 'Weather_Timestamp']

    print('accident_count = ', df.count())

    # convert the stirng timestamp column to date
    df = df.withColumn('weather_condition_datetime', F.to_date(F.col('Weather_Timestamp')))

    # extract weather_conditions table
    weather_conditions_table = df.select(
        F.monotonically_increasing_id().alias('weather_condition_id'),
        F.col('Weather_Condition').alias('condition')
    )

    # leave only unique conditions
    weather_conditions_table = weather_conditions_table.dropDuplicates(subset=['condition'])

    # save weather_conditions table
    weather_conditions_table.write.parquet(os.path.join(output_data, 'weather_conditions'), 'overwrite')

    # read in song data to use for city table
    city_df = spark.read.parquet(os.path.join(output_data, 'cities/*.parquet'))

    # read in song data to use for airport table
    airport_df = spark.read.parquet(os.path.join(output_data, 'airports/state=*/*.parquet'))

    joined_df = df.join(city_df, (df.City == city_df.city_name) & (df.State == city_df.state_code), how='inner')
    joined_df = joined_df.join(airport_df, 'airport_code', how='left')
    joined_df = joined_df.join(weather_conditions_table,
                               joined_df.Weather_Condition == weather_conditions_table.condition, how='left')

    print('joined_df_count = ', joined_df.count())

    # convert string timestamp to datetime
    joined_df = joined_df.withColumn('datetime', F.to_date(F.col('Start_Time')))

    # extract columns to create accidents table
    accident_table = joined_df.select(
        F.col('ID').alias('accident_id'),
        F.year('datetime').alias('year'),
        F.month('datetime').alias('month'),
        'datetime',
        F.col('Severity').alias('severity'),
        F.col('Distance(mi)').alias('distance'),
        F.col('Description').alias('description'),
        F.col('Temperature(F)').alias('temperature'),
        F.col('Wind_Speed(mph)').alias('wind_speed'),
        F.col('Precipitation(in)').alias('precipitation'),
        F.col('Airport_Code').alias('airport_code'),
        'city_id',
        'weather_condition_id'
    )

    accident_table.show(5)
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

    process_accident_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
