from pydantic_settings import BaseSettings
from typing import Optional, List
import os


class Settings(BaseSettings):
    # Kafka Configuration
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: Optional[str] = None
    kafka_sasl_username: Optional[str] = None
    kafka_sasl_password: Optional[str] = None
    
    # S3 Configuration
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_region: str = "us-east-1"
    s3_bucket_name: str = "zoo-digital-platform"
    s3_raw_prefix: str = "raw"
    s3_silver_prefix: str = "silver"
    s3_gold_prefix: str = "gold"
    
    # Data Sources Configuration
    zoo_api_base_url: str = "https://api.zoo.example.com"
    zoo_api_key: Optional[str] = None
    weather_api_key: Optional[str] = None
    weather_api_url: str = "https://api.openweathermap.org/data/2.5"
    
    # Ingestion Topics
    animal_data_topic: str = "zoo.animals"
    visitor_data_topic: str = "zoo.visitors"
    weather_data_topic: str = "zoo.weather"
    sensor_data_topic: str = "zoo.sensors"
    feeding_data_topic: str = "zoo.feeding"
    
    # Batch Processing
    batch_size: int = 1000
    batch_timeout_seconds: int = 60
    
    # Monitoring
    prometheus_port: int = 8000
    log_level: str = "INFO"
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Global settings instance
settings = Settings() 