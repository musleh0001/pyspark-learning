from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[3]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = Field(default="pyspark-learning")
    environment: str = Field(default="development")

    spark_master: str = Field(default="local[*]")
    spark_log_level: str = Field(default="WARN")

    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    kafka_orders_topic: str = Field(default="orders")
    kafka_customers_topic: str = Field(default="customers")

    input_path: str = Field(default="data/raw")
    bronze_path: str = Field(default="data/bronze")
    silver_path: str = Field(default="data/silver")
    gold_path: str = Field(default="data/gold")
    checkpoint_path: str = Field(default="data/checkpoints")
    quarantine_path: str = Field(default="data/quarantine")


settings = Settings()
