import os

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='../.env', extra='ignore')

    logs_dir: str = Field(alias="LOGS_DIR", default="logs")
    logging_file_path: str = Field(alias="LOGGING_FILE_PATH", default="logging.ini")
    data_path: str = Field(alias="DATA_PATH", default="data")
