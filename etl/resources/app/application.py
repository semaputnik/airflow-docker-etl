import sys

from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_timestamp

def preprocess_chat_messages(df: DataFrame) -> DataFrame:
    return (df.select(
        col("orderId").alias("order_id"),
        to_timestamp(col("messageSentTime")).alias("message_sent_time"),
        col("orderStage").alias("order_stage"),
        F.regexp_extract(df['senderAppType'], r'^([\w]+)', 1).alias('sender_type')
    ))


def preprocess_orders(df: DataFrame) -> DataFrame:
    return (df.select(
        col("orderId").alias("order_id"),
        col("cityCode").alias("city_code"),
    ))


def get_aggregations(df: DataFrame) -> list[Column]:
    aggregations: list[Column] = [
        F.first(df['city_code']).alias('city_code'),
        F.min(F.when(df['sender_type'] == 'Courier', df['message_sent_time'])).alias(
            'first_courier_message'),
        F.min(F.when(df['sender_type'] == 'Customer', df['message_sent_time'])).alias(
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


def aggregate_data(df: DataFrame) -> DataFrame:
    aggregations: list[Column] = get_aggregations(df)

    return df.groupBy(["order_id"]).agg(*aggregations)


def process() -> None:
    source_path: str = sys.argv[1]
    destination_path: str = sys.argv[2]

    spark = SparkSession.builder \
        .appName("Calculate_customer_courier_conversations") \
        .getOrCreate()

    chat_messages_file_path: str = f"{source_path}/customer_courier_chat_messages_2024_01_10.json"
    orders_file_path: str = f"{source_path}/orders_2024_01_10.json"

    df_chat: DataFrame = spark.read.option("multiline", "true").json(chat_messages_file_path)

    df_chat_preprocessed = preprocess_chat_messages(df_chat)

    df_orders = spark.read.option("multiline", "true").json(orders_file_path)

    df_orders_preprocessed = preprocess_orders(df_orders)

    df_joined = df_chat_preprocessed.join(df_orders_preprocessed, "order_id")

    aggregated_df = aggregate_data(df_joined)

    aggregated_df.write.option("header", True).mode("overwrite").csv(f"{destination_path}/aggregated_data")

    # aggregated_df.write.mode("overwrite").parquet(f"{settings.data_path_processed}/aggregated_data.json")

    spark.stop()


def main():
    process()


if __name__ == "__main__":
    main()
