from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


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

    input_path: str = Field(default="data/raw")
    bronze_path: str = Field(default="data/bronze")
    silver_path: str = Field(default="data/silver")
    gold_path: str = Field(default="data/gold")


settings = Settings()
