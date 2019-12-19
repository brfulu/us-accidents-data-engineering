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


def process_city_data(spark, input_data, output_data):
    """Process city data

    Extract transform and load the city dataset into an optimized data lake on S3.

    :param spark: spark session object
    :param input_data: string; input_data path
    :param output_data: string; output_data path
    """

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

    city_table.write.parquet(os.path.join(output_data, 'cities'), 'overwrite')


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

    process_city_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
