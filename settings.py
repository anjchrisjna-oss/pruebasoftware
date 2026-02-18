from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    app_name: str = "Tenebrio Farm"
    db_url: str = "sqlite:///./tenebrio_farm.db"


settings = Settings()