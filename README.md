# Customer-courier conversations

## Table of contents

1. [How to run](#how-to)
    1. [Prerequisites](#prerequisites)
    2. [Makefile commands](#make)
    3. [Run end-to-end](#run-end)
    4. [Run only customer_courier_conversations table calculation](#run-calculation)
2. [Project structure](#project-structure)
3. [Data Quality checks](#dq-checks)
4. [Orchestration](#orchestration)
5. [Late arriving events](#late-arriving-events)
6. [Improvements](#improvements)

## How to run <a name="how-to"></a>

### Prerequisites <a name="prerequisites"></a>

1. Install Make

``` 
brew install make
```

2. Install Docker

### Makefile commands <a name="make"></a>

All commands described in Makefile.

1. ___clean_data___ - remove ___logs___ and ___data___ folders
2. ___prepare___ - create ___logs___ and ___data___ folders, directories inside them
3. ___generate_data___ - generate source data. Data will be stored at ./data/source folder
4. ___setup_spark___ - setup spark master and worker containers
5. ___run_spark_application___ - run spark application to calculate customer_courier_conversations table. Data will be
   stored at ./data/processed folder and will be overwritten if exists.
6. ___shutdown_spark___ - shutdown spark master and worker containers

### Run end-to-end <a name="run-end"></a>

To run end-to-end you need to run all commands from the section [Makefile commands](#make) in the same order.

### Run only customer_courier_conversations table calculation <a name="run-calculation"></a>
I left generated data in the repository, so you can run only customer_courier_conversations table calculation.

Run the following commands:

1. ___setup_spark___
2. ___run_spark_application___
3. ___shutdown_spark___

Data will be stored at ./data/processed folder. Previous calculated data will be overwritten.

## Project structure <a name="project-structure"></a>

* ___data___ - folder for source and processed data. Processed data contains calculated data in different format (csv
  and parquet). I used csv for testing purposes and leaved files to show this ability. In real life I would use parquet
  format, because it's more efficient for storing big data.
* ___logs___ - folder for logs
* ___etl___ - folder spark application and docker files to setup spark environment
* ___test-data-generation___ - folder with test data generator

## Data Quality checks <a name="dq-checks"></a>

I suggest to use perform data quality checks and for the source data and for the destination data. For example, we can
implement the following checks:

|             Table              |              Field               |                Rule                |
|:------------------------------:|:--------------------------------:|:----------------------------------:|
| customer_courier_chat_messages |          senderAppType           |          Should be string          |
| customer_courier_chat_messages |          senderAppType           |    Should be in expected values    |
| customer_courier_chat_messages |          senderAppType           |         Should be not null         |
| customer_courier_chat_messages |          senderAppType           |        Should be not empty         |
| customer_courier_chat_messages |            customerId            |        Should be not empty         |
| customer_courier_chat_messages |            customerId            |         Should be not null         |
| customer_courier_chat_messages |            courierId             |         Should be numeric          |
| customer_courier_chat_messages |              fromId              |        Should be not empty         |
| customer_courier_chat_messages |              fromId              |         Should be not null         |
| customer_courier_chat_messages |              fromId              |         Should be numeric          |
| customer_courier_chat_messages |               toId               |        Should be not empty         |
| customer_courier_chat_messages |               toId               |         Should be not null         |
| customer_courier_chat_messages |               toId               |         Should be numeric          |
| customer_courier_chat_messages |             orderId              |        Should be not empty         |
| customer_courier_chat_messages |             orderId              |         Should be not null         |
| customer_courier_chat_messages |             orderId              |         Should be numeric          |
| customer_courier_chat_messages |            orderStage            |          Should be string          |
| customer_courier_chat_messages |            orderStage            |    Should be in expected values    |
| customer_courier_chat_messages |            orderStage            |         Should be not null         |
| customer_courier_chat_messages |            orderStage            |        Should be not empty         |
| customer_courier_chat_messages |            courierId             |        Should be not empty         |
| customer_courier_chat_messages |            courierId             |         Should be not null         |
| customer_courier_chat_messages |            courierId             |         Should be numeric          |
| customer_courier_chat_messages |         messageSentTime          |        Should be not empty         |
| customer_courier_chat_messages |         messageSentTime          |         Should be not null         |
| customer_courier_chat_messages |         messageSentTime          |        Should be timestamp         |
|             orders             |             cityCode             |          Should be string          |
|             orders             |             cityCode             |    Should be in expected values    |
|             orders             |             cityCode             |         Should be not null         |
|             orders             |             cityCode             |        Should be not empty         |
|             orders             |             orderId              |        Should be not empty         |
|             orders             |             orderId              |         Should be not null         |
|             orders             |             orderId              |         Should be numeric          |
| customer_courier_conversations |             order_id             |        Should be not empty         |
| customer_courier_conversations |             order_id             |         Should be not null         |
| customer_courier_conversations |             order_id             |         Should be numeric          |
| customer_courier_conversations |            city_code             |          Should be string          |
| customer_courier_conversations |            city_code             |    Should be in expected values    |
| customer_courier_conversations |            city_code             |         Should be not null         |
| customer_courier_conversations |            city_code             |        Should be not empty         |
| customer_courier_conversations |      first_courier_message       |    Should be timestamp on null     |
| customer_courier_conversations |      first_customer_message      |    Should be timestamp on null     |
| customer_courier_conversations |       num_messages_courier       |     Should be positive numeric     |
| customer_courier_conversations |      num_messages_customer       |     Should be positive numeric     |
| customer_courier_conversations |         first_message_by         |          Should be string          |
| customer_courier_conversations |         first_message_by         |    Should be in expected values    |
| customer_courier_conversations |         first_message_by         |         Should be not null         |
| customer_courier_conversations |         first_message_by         |        Should be not empty         |
| customer_courier_conversations |     conversation_started_at      |        Should be not empty         |
| customer_courier_conversations |     conversation_started_at      |         Should be not null         |
| customer_courier_conversations |     conversation_started_at      |        Should be timestamp         |
| customer_courier_conversations | first_responsetime_delay_seconds | Should be positive numeric on null |
| customer_courier_conversations |        last_message_time         |        Should be not empty         |
| customer_courier_conversations |        last_message_time         |         Should be not null         |
| customer_courier_conversations |        last_message_time         |        Should be timestamp         |
| customer_courier_conversations |     last_message_order_stage     |          Should be string          |
| customer_courier_conversations |     last_message_order_stage     |    Should be in expected values    |
| customer_courier_conversations |     last_message_order_stage     |         Should be not null         |
| customer_courier_conversations |     last_message_order_stage     |        Should be not empty         |

To perform this check I suggest to use [Great Expectations](https://greatexpectations.io/). It's a widely used tool, where we can define rules for data quality checks and run them on schedule or on demand. Also it has integrations with Airflow and DBT.

## Orchestration <a name="orchestration"></a>

I tried to implement orchestration using Airflow, but my working machine is not powerful enough to run Airflow and Spark at the same time. So I decided to use Makefile to run Spark application.
In real life I would use Airflow to orchestrate Spark jobs.

Dag can look like this:

```
with DAG('customer_courier_conversations',
          default_args=default_args,
          description='Calculate customer_courier_conversations report',
          start_date=datetime.now(),
          schedule_interval='@hourly'
          ) as dag:

    start = DummyOperator(task_id="start", dag=dag)

    spark_job = SparkSubmitOperator(
        task_id="spark_job",
        application="/usr/local/spark/app/application.py",
        name=spark_app_name,
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master": spark_master},
        application_args=[source_data_path, destination_data_path],
        dag=dag
    )

    end = DummyOperator(task_id="end", dag=dag)

    start >> spark_job >> end
```

Data checks can be performed as a tasks before and after spark-submit task.
Also, Airflow can be very useful for monitoring and alerting.

## Late arriving events <a name="late-arriving-events"></a>

To handle late arriving events we can use lambda architecture. We can store data in two places: batch layer and speed layer. Batch layer will store all data and speed layer will store only last 24 hours of data. Then we can merge data from batch and speed layers and calculate customer_courier_conversations table.

## Improvements <a name="improvements"></a>

1. Implement data quality checks
2. Implement orchestration using Airflow
3. Optimise data storage structure. For example, we can store data in parquet format and partition it by date.
4. Implement monitoring and alerting
5. Implement e-2-e tests
6. Setup CI/CD