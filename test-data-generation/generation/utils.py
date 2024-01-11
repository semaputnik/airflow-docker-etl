import logging
from datetime import datetime, timedelta
from random import choice, randint, choices

city_codes = ["AGP", "ALC", "LEI", "OVD", "BCN", "BIO", "ODB", "LCG", "YJD", "GRO",
              "LPA", "SLM", "SDR", "SCQ", "TFS", "TFN", "VLC", "VLL", "VGO", "VIT",
              "GRX", "IBZ", "XRY", "QGJ", "MJV", "MAD", "MAH", "PMI", "PNA", "REU", "ZAZ"]

stages = {1: "PICKING_UP", 2: "DELIVERING", 3: "DELIVERED"}


def get_datetime() -> datetime:
    start_date = datetime.now()
    changed = start_date.replace(year=2024, month=1, day=1)
    return (changed + timedelta(days=randint(0, 7))
            + timedelta(hours=randint(0, 12))
            + timedelta(hours=randint(0, 60)))


def get_next_datetime(current_datetime: datetime) -> datetime:
    return current_datetime + timedelta(minutes=randint(1, 60))


def get_order_stage(stage_index: int) -> str:
    return stages[stage_index]


def get_order_stage_index(current_stage_index: int = None) -> int:
    stage_indexes = range(1, len(stages) + 1)

    if current_stage_index:
        if current_stage_index == 3:
            return 3
        elif choices([True, False], weights=[1, 10], k=1)[0]:
            logging.info(f"Update current stage index: {current_stage_index}")
            return current_stage_index + 1
        else:
            return current_stage_index
    else:
        return choices(stage_indexes, weights=[10, 4, 1], k=1)[0]


def get_customer_app_type() -> str:
    return choice(["Customer iOS", "Customer Android"])


def get_courier_app_type() -> str:
    return choice(["Courier iOS", "Courier Android"])


def get_id() -> int:
    lower_bound = 10000000
    upper_bound = 99999999
    return randint(lower_bound, upper_bound)


def get_city_code() -> str:
    return choice(city_codes)


def is_customer_message() -> bool:
    return choice([True, False])
