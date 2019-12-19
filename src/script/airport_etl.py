import configparser
from datetime import datetime
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_airport_data(spark, input_data, output_data):
    """Process airport data

    Extract transform and load the airport dataset into an optimized data lake on S3.

    :param spark: spark session object
    :param input_data: string; input_data path
    :param output_data: string; output_data path
    """

    # get filepath to airport data file
    airport_data = os.path.join(input_data, 'airport_data/*.csv')

    # read airport data files
    df = spark.read.csv(airport_data, header=True)
    df = df.filter(df.continent == 'NA')
    df = df.filter(df.iso_country == 'US')

    # extract 2-letter state code
    extract_state_code = F.udf(lambda x: x[3:], StringType())
    df = df.withColumn('state_code', extract_state_code('iso_region'))

    print('airport_coint = ', df.count())

    # extract columns to create songs table
    airport_table = df.select(
        F.col('ident').alias('airport_code'),
        F.col('state_code').alias('state'),
        'state_code',
        'type',
        'name',
        'municipality'
    )

    airport_table.write.partitionBy('state').parquet(os.path.join(output_data, 'airports'), 'overwrite')


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

    process_airport_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
