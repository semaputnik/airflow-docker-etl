import logging
from logging.config import fileConfig
from typing import List

from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_timestamp

from settings import Settings

fileConfig(Settings().logging_file_path, defaults={'logfilename': f"{Settings().logs_dir}/spark_application.log"})


def preprocess_chat_messages(df: DataFrame) -> DataFrame:
    """
    Select only needed columns from chat messages data
    :param df: DataFrame - source chat messages data
    :return: DataFrame - preprocessed chat messages data
    """
    logging.info("Preprocess chat messages data")
    return (df.select(
        col("orderId").alias("order_id"),
        to_timestamp(col("messageSentTime")).alias("message_sent_time"),
        col("orderStage").alias("order_stage"),
        F.regexp_extract(df['senderAppType'], r'^([\w]+)', 1).alias('sender_type')
    ))


def preprocess_orders(df: DataFrame) -> DataFrame:
    """
    Select only needed columns from orders data
    :param df: DataFrame - source orders data
    :return: DataFrame - preprocessed orders data
    """
    logging.info("Preprocess orders data")
    return (df.select(
        col("orderId").alias("order_id"),
        col("cityCode").alias("city_code"),
    ))


def get_aggregations(df: DataFrame) -> List[Column]:
    """
    Get aggregations list for data
    :param df: DataFrame - source joined data
    :return: List[Column] - aggregations list
    """
    logging.info("Setup data aggregations")
    aggregations: List[Column] = [
        F.first(df['city_code']).alias('city_code'),
        F.min(F.when(df['sender_type'] == 'Courier', df['message_sent_time']).otherwise(None)).alias(
            'first_courier_message'),
        F.min(F.when(df['sender_type'] == 'Customer', df['message_sent_time']).otherwise(None)).alias(
            'first_customer_message'),
        F.sum(F.when(df['sender_type'] == 'Courier', 1).otherwise(0)).alias('num_messages_courier'),
        F.sum(F.when(df['sender_type'] == 'Customer', 1).otherwise(0)).alias('num_messages_customer'),
        F.first(df['sender_type']).alias('first_message_by'),
        F.min(df['message_sent_time']).alias('conversation_started_at'),
        F.max(df['message_sent_time']).alias('last_message_time'),
        F.last(df['order_stage']).alias('last_message_order_stage')
    ]

    time_diff = F.unix_timestamp('first_customer_message') - F.unix_timestamp('first_courier_message')
    aggregations.append(F.abs(time_diff).alias('first_response_time_delay_seconds'))

    return aggregations


def aggregate_data(df: DataFrame) -> DataFrame:
    """
    Aggregate data by order_id
    :param df: DataFrame - source joined data
    :return: DataFrame - aggregated data
    """
    logging.info("Perform data aggregations")
    aggregations: list[Column] = get_aggregations(df)

    return df.groupBy(["order_id"]).agg(*aggregations)


def save_results(spark: SparkSession, df: DataFrame, destination_path: str, output_data_type: str) -> None:
    """
    Save results to destination path
    :param spark: SparkSession - spark session
    :param df: DataFrame - aggregated data
    :param destination_path: str - destination path
    :param output_data_type: str - output data type
    """
    logging.info("Start writing results")
    if output_data_type == "csv":
        (df.write.option("header", True)
         .mode("overwrite")
         .csv(f"{destination_path}/csv/customer_courier_conversations"))
    else:
        (df.write
         .mode("overwrite")
         .parquet(f"{destination_path}/parquet/customer_courier_conversations"))


def process(spark: SparkSession, settings: Settings) -> None:
    logging.info("Start processing")

    # Create source data paths
    chat_messages_file_path: str = f"{settings.source_data_path}/customer_courier_chat_messages*.json"
    orders_file_path: str = f"{settings.source_data_path}/orders*.json"

    # Load data
    df_chat: DataFrame = spark.read.option("multiline", "true").json(chat_messages_file_path)
    df_orders = spark.read.option("multiline", "true").json(orders_file_path)

    # Preprocess data
    df_chat_preprocessed = preprocess_chat_messages(df_chat)
    df_orders_preprocessed = preprocess_orders(df_orders)

    # Join data
    df_joined = df_chat_preprocessed.join(df_orders_preprocessed, "order_id")

    # Aggregate data
    aggregated_df = aggregate_data(df_joined)

    # Save results
    save_results(spark, aggregated_df, settings.destination_data_path, settings.output_data_type)


def main():
    settings = Settings()
    spark = SparkSession.builder \
        .appName("Calculate_customer_courier_conversations") \
        .getOrCreate()

    process(spark, settings)

    spark.stop()


if __name__ == "__main__":
    main()
