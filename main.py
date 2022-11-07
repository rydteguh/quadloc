from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
import pyspark.sql.functions as func
from pyspark.sql.functions import col, date_trunc, to_timestamp

import argparse
import os
import sys


def daily_average_users(spark, df):
    """
    Explanation : Number of distinct users(device_id) seen in a day.
    :param spark:
    :param df:
    :return dau:
    """

    df.createOrReplaceTempView("dau")
    dau = spark.sql("SELECT COUNT(DISTINCT device_id) as total_events,"
                    " CAST(timestamp AS date) "
                    "FROM dau"
                    " GROUP BY 2")

    return dau


def attribute_percentage_completeness(spark, df):
    """
    Explanation : Percentage of events with filled values.
    :param spark:
    :param df:
    :return:
    """

    df.createOrReplaceTempView("apc")
    apc = spark.sql("SELECT " \
          "SUM(CASE WHEN device_id IS NOT null THEN 1 ELSE 0 END)/COUNT(*) AS latitude_completeness, " \
          "SUM(CASE WHEN latitude IS NOT null THEN 1 ELSE 0 END)/COUNT(*) AS longitude_completeness, " \
          "SUM(CASE WHEN longitude IS NOT null THEN 1 ELSE 0 END)/COUNT(*) AS longitude_completeness, " \
          "SUM(CASE WHEN horizontal_accuracy IS NOT null THEN 1 ELSE 0 END)/COUNT(*) " \
          "AS horizontal_accuracy_completeness, " \
          "SUM(CASE WHEN timestamp IS NOT null THEN 1 ELSE 0 END)/COUNT(*) AS timestamp_completeness, " \
          "SUM(CASE WHEN ip_address IS NOT null THEN 1 ELSE 0 END)/COUNT(*) AS ip_address_completeness, " \
          "SUM(CASE WHEN device_os IS NOT null THEN 1 ELSE 0 END)/COUNT(*) AS device_os_completeness, " \
          "SUM(CASE WHEN os_version IS NOT null THEN 1 ELSE 0 END)/COUNT(*) AS os_version_completeness, " \
          "SUM(CASE WHEN user_agent IS NOT null THEN 1 ELSE 0 END)/COUNT(*) AS user_agent_completeness, " \
          "SUM(CASE WHEN country IS NOT null THEN 1 ELSE 0 END)/COUNT(*) AS country_completeness, " \
          "SUM(CASE WHEN source_id IS NOT null THEN 1 ELSE 0 END)/COUNT(*) AS source_id_completeness, " \
          "SUM(CASE WHEN publisher_id IS NOT null THEN 1 ELSE 0 END)/COUNT(*) AS publisher_id_completeness, " \
          "SUM(CASE WHEN app_id IS NOT null THEN 1 ELSE 0 END)/COUNT(*) AS app_id_completeness, " \
          "SUM(CASE WHEN location_context IS NOT null THEN 1 ELSE 0 END)/COUNT(*) AS location_context_completeness, " \
          "SUM(CASE WHEN geohash IS NOT null THEN 1 ELSE 0 END)/COUNT(*) AS geohash_completeness " \
          "FROM apc")

    return apc


def daily_average_event_per_user(spark, df):
    """
    Explanation : Average of total number of events for each unique user seen
    in a day
    :param spark:
    :param df:
    :return:
    """

    df.createOrReplaceTempView("dau_events")
    dau_events = spark.sql("SELECT device_id, CAST(timestamp AS date), count(*) as total_events " \
                 "FROM dau_events " \
                 "GROUP BY 1,2 ")

    return dau_events


def horizontal_accuracy_distribution(spark, df):
    """
    Explanation : Calculate the average horizontal accuracy for each User and
    sort them into the following bins.
    :param spark:
    :param df:
    :return:
    """
    df.createOrReplaceTempView("ha_distribution")
    ha_dist = spark.sql("""
                  WITH
                  total_user AS (
                    select count(distinct device_id) as total
                    from ha_distribution
                  ),
                
                  avg_ha AS (
                  SELECT
                    device_id,
                    AVG(horizontal_accuracy) AS avg_event
                  FROM
                    ha_distribution
                  GROUP BY
                    1
                  ORDER BY
                    1 ASC ),
                
                  range_calculation AS (
                  SELECT
                    CASE
                      WHEN avg_event > 500 THEN 501
                      WHEN avg_event > 101 THEN 101
                      WHEN avg_event > 51 THEN 51
                      WHEN avg_event > 25 THEN 26
                      WHEN avg_event > 10 THEN 11
                      WHEN avg_event > 5 THEN 6
                      WHEN avg_event <= 5 THEN 0
                  END
                    AS ha_range
                  FROM
                    avg_ha ),
                
                    range_grouping as (
                    SELECT
                      ha_range,
                      COUNT(*) AS total_user_per_range,
                      CASE
                        WHEN ha_range >= 501 THEN 'over 501'
                        WHEN ha_range >= 101 THEN '101 to 500'
                        WHEN ha_range >= 51 THEN '51 to 100'
                        WHEN ha_range >= 26 THEN '26 to 50'
                        WHEN ha_range >= 11 THEN '11 to 25'
                        WHEN ha_range >= 5 THEN '6 to 10'
                      ELSE
                      '0 to 5'
                    END
                      AS notes
                    FROM
                      range_calculation, total_user
                    GROUP BY
                      1
                    ),
                
                    range_percentage as (
                      select 
                      ha_range, 
                      total_user_per_range,
                      round((range_grouping.total_user_per_range / total_user.total)*100,1) AS percentage,
                      notes
                      from range_grouping, total_user
                    )
                    
                    SELECT 
                    ha_range, 
                    total_user_per_range,
                    percentage,
                    round(sum(range_percentage.percentage) over(order by ha_range),1) AS cumulative_percentage,
                    notes
                    from range_percentage
                """)

    return ha_dist


def main():
    spark = SparkSession.builder \
        .appName("csv-reader") \
        .getOrCreate()

    schema = StructType() \
        .add("device_id", StringType(), True) \
        .add("id_type", StringType(), True) \
        .add("latitude", DoubleType(), True) \
        .add("longitude", DoubleType(), True) \
        .add("horizontal_accuracy", DoubleType(), True) \
        .add("timestamp", LongType(), True) \
        .add("ip_address", StringType(), True) \
        .add("device_os", StringType(), True) \
        .add("os_version", StringType(), True) \
        .add("user_agent", StringType(), True) \
        .add("country", StringType(), True) \
        .add("source_id", StringType(), True) \
        .add("publisher_id", StringType(), True) \
        .add("app_id", StringType(), True) \
        .add("location_context", StringType(), True) \
        .add("geohash", StringType(), True)

    df = spark.read \
        .format("csv") \
        .schema(schema) \
        .load(args.source)

    df.write.parquet(args.destination)

    clean_df = df \
        .withColumn("latitude", func.round("latitude", 4)) \
        .withColumn("longitude", func.round("longitude", 4)) \
        .withColumn("timestamp", col("timestamp")/1000) \
        .withColumn("timestamp", to_timestamp("timestamp")) \
        .withColumn("timestamp", date_trunc("second", col("timestamp")))

    duplicate_count_df = clean_df \
        .groupBy("device_id", "latitude", "longitude", "timestamp") \
        .count() \
        .filter(col("count") > 1) \
        .count()

    dau = daily_average_users(spark, clean_df)
    apc = attribute_percentage_completeness(spark, clean_df)
    daepu = daily_average_event_per_user(spark, clean_df)
    ha_dist = horizontal_accuracy_distribution(spark, clean_df)

    duplicate_count_df
    dau.show()
    apc.show()
    daepu.show()
    ha_dist.show()


if __name__ == "__main__":

    try:
        parser = argparse.ArgumentParser(description="Load and Analysis File")
        parser.add_argument('--source', default=os.getenv("SOURCE_PATH"))
        parser.add_argument('--destination', default=os.getenv("DESTINATION_PATH"))
        args = parser.parse_args()
        main()

    except KeyboardInterrupt:
        sys.exit(0)




