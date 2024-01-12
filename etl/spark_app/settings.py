import logging
import os

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    print(os.getcwd())
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')

    logs_dir: str = Field(alias="LOGS_DIR")
    logging_file_path: str = Field(alias="LOGGING_FILE_PATH", default="logging.ini")
    source_data_path: str = Field(alias="SOURCE_DATA_PATH", default="data/source")
    destination_data_path: str = Field(alias="DESTINATION_DATA_PATH", default="data/processed")
    output_data_type: str = Field(alias="OUTPUT_DATA_TYPE", default="csv")
