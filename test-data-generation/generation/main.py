import datetime
import json
import logging
from logging import getLogger
from logging.config import fileConfig
from random import randint

import pandas as pd

from settings import Settings
from utils import get_id, get_city_code, get_customer_app_type, get_courier_app_type, is_customer_message, \
    get_order_stage, get_next_datetime, get_datetime, get_order_stage_index

settings = Settings()

fileConfig(settings.logging_file_path, defaults={'logfilename': f"{settings.logs_dir}/test_data_generation.log"})
logger = getLogger(__name__)


def generate_conversation(
        customer_id: int, courier_id: int, order_id: int,
        customer_app_type: str, courier_app_type: str, city_code: str):
    amount_of_messages = randint(1, 10)

    conversation_started = True
    current_stage_index = None
    message_sent_time = get_datetime()

    messages = []

    for num_of_message in range(amount_of_messages):
        sender_flag = is_customer_message()

        sender_app_type = customer_app_type if sender_flag else courier_app_type
        from_id = customer_id if sender_flag else courier_id
        to_id = courier_id if sender_flag else customer_id
        current_stage_index = get_order_stage_index(current_stage_index)
        current_stage = get_order_stage(current_stage_index)
        message_sent_time = get_next_datetime(message_sent_time)

        msg = {
            "senderAppType": sender_app_type,
            "customerId": customer_id,
            "fromId": from_id,
            "toId": to_id,
            "chatStartedByMessage": conversation_started,
            "orderId": order_id,
            "orderStage": current_stage,
            "courierId": courier_id,
            "messageSentTime": message_sent_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        }

        messages.append(msg)

        conversation_started = False

    return messages


def generate_order(order_id: int, city_code: str):
    return {
        "orderId": order_id,
        "cityCode": city_code
    }


def save_data_to_file(data: list, file_name: str):
    date_prefix: str = datetime.datetime.now().strftime("%Y_%m_%d")
    data_path: str = f"{settings.data_path}/{file_name}_{date_prefix}.json"
    logging.info(f"Saving data to file: {data_path}")
    with open(data_path, "w") as file:
        json.dump(data, file, indent=2)


def generate_data(amount_of_conversations: int):
    customer_courier_chat_messages_content = []
    orders_content = []

    for i in range(amount_of_conversations):
        customer_id: int = get_id()
        courier_id: int = get_id()
        order_id: int = get_id()
        customer_app_type: str = get_customer_app_type()
        courier_app_type: str = get_courier_app_type()
        city_code: str = get_city_code()

        conversation = generate_conversation(
            customer_id, courier_id, order_id,
            customer_app_type, courier_app_type, city_code
        )

        customer_courier_chat_messages_content.extend(conversation)

        logging.info(f"Amount of messages for order {order_id}: {len(customer_courier_chat_messages_content)}")

        order = generate_order(order_id, city_code)

        orders_content.append(order)

    save_data_to_file(
        data=customer_courier_chat_messages_content,
        file_name="customer_courier_chat_messages"
    )

    save_data_to_file(
        data=orders_content,
        file_name="orders"
    )


def main():
    logging.info("Start generating data")
    generate_data(100)


if __name__ == "__main__":
    main()
