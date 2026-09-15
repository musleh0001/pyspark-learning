from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "pyspark-learning"

    input_data_path: str = "data/raw/customers.csv"
    output_data_path: str = "data/output/customers"

    spark_master: str = "local[*]"
    spark_log_level: str = "WARN"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
