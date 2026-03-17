from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


ROOT_DIR = Path(__file__).resolve().parents[3]
CONFIG_FILE = ROOT_DIR / "configs" / "base.yml"


class ProjectConfig(BaseModel):
    name: str
    environment: str


class StorageConfig(BaseModel):
    root: str
    landing: str
    bronze: str
    silver: str
    gold: str
    checkpoints: str
    warehouse: str
    logs: str


class SparkConfig(BaseModel):
    app_name: str
    master: str
    warehouse_dir: str
    driver_memory: str
    executor_memory: str
    delta_package: str


class PostgresConfig(BaseModel):
    host: str
    port: int
    database: str
    user: str
    password: str


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    project_name: str = Field(alias="PROJECT_NAME")
    environment: str = Field(alias="ENVIRONMENT")
    log_level: str = Field(alias="LOG_LEVEL")
    pipeline_run_id: str = Field(alias="PIPELINE_RUN_ID")

    storage_root: str = Field(alias="STORAGE_ROOT")
    landing_root: str = Field(alias="LANDING_ROOT")
    bronze_root: str = Field(alias="BRONZE_ROOT")
    silver_root: str = Field(alias="SILVER_ROOT")
    gold_root: str = Field(alias="GOLD_ROOT")
    checkpoint_root: str = Field(alias="CHECKPOINT_ROOT")
    logs_root: str = Field(alias="LOGS_ROOT")

    spark_app_name: str = Field(alias="SPARK_APP_NAME")
    spark_master: str = Field(alias="SPARK_MASTER")
    spark_warehouse_dir: str = Field(alias="SPARK_WAREHOUSE_DIR")
    spark_driver_memory: str = Field(alias="SPARK_DRIVER_MEMORY")
    spark_executor_memory: str = Field(alias="SPARK_EXECUTOR_MEMORY")
    delta_package: str = Field(alias="DELTA_PACKAGE")

    postgres_host: str = Field(alias="POSTGRES_HOST")
    postgres_port: int = Field(alias="POSTGRES_PORT")
    postgres_db: str = Field(alias="POSTGRES_DB")
    postgres_user: str = Field(alias="POSTGRES_USER")
    postgres_password: str = Field(alias="POSTGRES_PASSWORD")

    @classmethod
    def from_sources(cls, defaults: dict[str, Any]) -> "Settings":
        project = defaults["project"]
        storage = defaults["storage"]
        spark = defaults["spark"]
        postgres = defaults["postgres"]

        return cls(
            PROJECT_NAME=project["name"],
            ENVIRONMENT=project["environment"],
            LOG_LEVEL="INFO",
            PIPELINE_RUN_ID="manual_local_run",
            STORAGE_ROOT=storage["root"],
            LANDING_ROOT=storage["landing"],
            BRONZE_ROOT=storage["bronze"],
            SILVER_ROOT=storage["silver"],
            GOLD_ROOT=storage["gold"],
            CHECKPOINT_ROOT=storage["checkpoints"],
            LOGS_ROOT=storage["logs"],
            SPARK_APP_NAME=spark["app_name"],
            SPARK_MASTER=spark["master"],
            SPARK_WAREHOUSE_DIR=spark["warehouse_dir"],
            SPARK_DRIVER_MEMORY=spark["driver_memory"],
            SPARK_EXECUTOR_MEMORY=spark["executor_memory"],
            DELTA_PACKAGE=spark["delta_package"],
            POSTGRES_HOST=postgres["host"],
            POSTGRES_PORT=postgres["port"],
            POSTGRES_DB=postgres["database"],
            POSTGRES_USER=postgres["user"],
            POSTGRES_PASSWORD=postgres["password"],
        )

    @property
    def storage(self) -> StorageConfig:
        return StorageConfig(
            root=self.storage_root,
            landing=self.landing_root,
            bronze=self.bronze_root,
            silver=self.silver_root,
            gold=self.gold_root,
            checkpoints=self.checkpoint_root,
            warehouse=self.spark_warehouse_dir,
            logs=self.logs_root,
        )

    @property
    def spark(self) -> SparkConfig:
        return SparkConfig(
            app_name=self.spark_app_name,
            master=self.spark_master,
            warehouse_dir=self.spark_warehouse_dir,
            driver_memory=self.spark_driver_memory,
            executor_memory=self.spark_executor_memory,
            delta_package=self.delta_package,
        )

    @property
    def postgres(self) -> PostgresConfig:
        return PostgresConfig(
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
            user=self.postgres_user,
            password=self.postgres_password,
        )

    def ensure_directories(self) -> None:
        """Garante a existencia dos diretorios-base do projeto."""
        directories = {
            self.storage_root,
            self.landing_root,
            self.bronze_root,
            self.silver_root,
            self.gold_root,
            self.checkpoint_root,
            self.logs_root,
            self.spark_warehouse_dir,
        }
        for directory in directories:
            resolve_path(directory).mkdir(parents=True, exist_ok=True)


def load_defaults(config_file: Path = CONFIG_FILE) -> dict[str, Any]:
    with config_file.open("r", encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def resolve_path(path_value: str) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return ROOT_DIR / path


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    defaults = load_defaults()
    return Settings.from_sources(defaults)
