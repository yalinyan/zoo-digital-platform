#!/usr/bin/env python3
"""
Zoo Digital Platform - Data Ingestion System
Main entry point for the Kafka/S3 ingestion pipeline.
"""

import time
import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any
from loguru import logger
import signal
import sys

from config.settings import settings
from data.ingestion.ingestion_orchestrator import orchestrator
from data.ingestion.models import AnimalSpecies, AnimalStatus, WeatherCondition


class SampleDataGenerator:
    """Generates sample data for testing the ingestion system."""
    
    def __init__(self):
        self.animal_names = [
            "Leo", "Simba", "Nala", "Mufasa", "Sarabi", "Timon", "Pumbaa",
            "Dumbo", "Babar", "Horton", "Gerald", "Melman", "Alex", "Marty",
            "Gloria", "Julian", "King Julian", "Maurice", "Mort", "Rico"
        ]
        
        self.enclosures = [
            "Lion_Exhibit_A", "Lion_Exhibit_B", "Elephant_Habitat", 
            "Giraffe_Enclosure", "Penguin_Pool", "Gorilla_Habitat",
            "Zebra_Plains", "Rhino_Exhibit", "Panda_Garden", "Kangaroo_Outback"
        ]
        
        self.visitor_names = [
            "John Smith", "Jane Doe", "Bob Johnson", "Alice Brown", "Charlie Wilson",
            "Diana Davis", "Edward Miller", "Fiona Garcia", "George Martinez", "Helen Rodriguez"
        ]
    
    def generate_animal_data(self, count: int = 10) -> List[Dict[str, Any]]:
        """Generate sample animal data."""
        data = []
        
        for i in range(count):
            animal = {
                "animal_id": f"ANM_{uuid.uuid4().hex[:8].upper()}",
                "name": random.choice(self.animal_names),
                "species": random.choice(list(AnimalSpecies)).value,
                "age": random.randint(1, 25),
                "weight": round(random.uniform(50, 5000), 2),
                "status": random.choice(list(AnimalStatus)).value,
                "enclosure_id": random.choice(self.enclosures),
                "last_feeding": datetime.utcnow() - timedelta(hours=random.randint(1, 12)),
                "temperature": round(random.uniform(35, 42), 1),
                "heart_rate": random.randint(60, 120),
                "timestamp": datetime.utcnow()
            }
            data.append(animal)
        
        return data
    
    def generate_visitor_data(self, count: int = 20) -> List[Dict[str, Any]]:
        """Generate sample visitor data."""
        data = []
        
        for i in range(count):
            entry_time = datetime.utcnow() - timedelta(hours=random.randint(1, 8))
            exit_time = entry_time + timedelta(hours=random.randint(1, 4)) if random.random() > 0.3 else None
            
            visitor = {
                "visitor_id": f"VIS_{uuid.uuid4().hex[:8].upper()}",
                "ticket_type": random.choice(["adult", "child", "senior", "family"]),
                "entry_time": entry_time,
                "exit_time": exit_time,
                "age_group": random.choice(["child", "teen", "adult", "senior"]),
                "group_size": random.randint(1, 6),
                "visited_enclosures": random.sample(self.enclosures, random.randint(1, 5)),
                "total_spent": round(random.uniform(20, 200), 2),
                "satisfaction_rating": random.randint(1, 5) if random.random() > 0.2 else None,
                "timestamp": datetime.utcnow()
            }
            data.append(visitor)
        
        return data
    
    def generate_weather_data(self, count: int = 1) -> List[Dict[str, Any]]:
        """Generate sample weather data."""
        data = []
        
        for i in range(count):
            weather = {
                "location": "Zoo Central",
                "temperature": round(random.uniform(-10, 35), 1),
                "humidity": round(random.uniform(30, 90), 1),
                "pressure": round(random.uniform(980, 1030), 1),
                "wind_speed": round(random.uniform(0, 25), 1),
                "wind_direction": random.randint(0, 360),
                "condition": random.choice(list(WeatherCondition)).value,
                "visibility": round(random.uniform(1, 20), 1),
                "uv_index": round(random.uniform(0, 11), 1),
                "timestamp": datetime.utcnow()
            }
            data.append(weather)
        
        return data
    
    def generate_sensor_data(self, count: int = 15) -> List[Dict[str, Any]]:
        """Generate sample sensor data."""
        data = []
        
        sensor_types = ["temperature", "humidity", "air_quality", "water_ph", "light", "noise"]
        
        for i in range(count):
            sensor_type = random.choice(sensor_types)
            sensor = {
                "sensor_id": f"SEN_{uuid.uuid4().hex[:8].upper()}",
                "sensor_type": sensor_type,
                "enclosure_id": random.choice(self.enclosures),
                "temperature": round(random.uniform(15, 35), 1) if sensor_type == "temperature" else None,
                "humidity": round(random.uniform(40, 80), 1) if sensor_type == "humidity" else None,
                "air_quality": round(random.uniform(0, 150), 1) if sensor_type == "air_quality" else None,
                "water_ph": round(random.uniform(6.5, 8.5), 1) if sensor_type == "water_ph" else None,
                "water_temperature": round(random.uniform(10, 25), 1) if sensor_type == "water_ph" else None,
                "light_intensity": round(random.uniform(100, 1000), 1) if sensor_type == "light" else None,
                "noise_level": round(random.uniform(30, 80), 1) if sensor_type == "noise" else None,
                "battery_level": round(random.uniform(20, 100), 1),
                "timestamp": datetime.utcnow()
            }
            data.append(sensor)
        
        return data
    
    def generate_feeding_data(self, count: int = 8) -> List[Dict[str, Any]]:
        """Generate sample feeding data."""
        data = []
        
        food_types = ["meat", "vegetables", "fruits", "fish", "insects", "hay", "pellets"]
        
        for i in range(count):
            feeding = {
                "feeding_id": f"FED_{uuid.uuid4().hex[:8].upper()}",
                "animal_id": f"ANM_{uuid.uuid4().hex[:8].upper()}",
                "food_type": random.choice(food_types),
                "quantity": round(random.uniform(0.5, 10), 2),
                "feeding_time": datetime.utcnow() - timedelta(hours=random.randint(0, 6)),
                "keeper_id": f"KEEPER_{random.randint(1, 10)}",
                "notes": random.choice([None, "Regular feeding", "Special diet", "Medication added"]),
                "consumed_amount": round(random.uniform(0.3, 8), 2) if random.random() > 0.2 else None,
                "timestamp": datetime.utcnow()
            }
            data.append(feeding)
        
        return data


def setup_logging():
    """Setup logging configuration."""
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=settings.log_level
    )
    logger.add(
        "logs/ingestion.log",
        rotation="1 day",
        retention="30 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level=settings.log_level
    )


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info("Received shutdown signal, stopping ingestion...")
    orchestrator.stop()
    sys.exit(0)


def main():
    """Main entry point for the ingestion system."""
    # Setup logging
    setup_logging()
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("Starting Zoo Digital Platform - Data Ingestion System")
    
    # Initialize sample data generator
    data_generator = SampleDataGenerator()
    
    # Register data sources with the orchestrator
    orchestrator.register_data_source('animals', lambda: data_generator.generate_animal_data(5))
    orchestrator.register_data_source('visitors', lambda: data_generator.generate_visitor_data(10))
    orchestrator.register_data_source('weather', lambda: data_generator.generate_weather_data(1))
    orchestrator.register_data_source('sensors', lambda: data_generator.generate_sensor_data(8))
    orchestrator.register_data_source('feeding', lambda: data_generator.generate_feeding_data(4))
    
    try:
        # Start the orchestrator
        orchestrator.start()
        
        logger.info("Ingestion system is running. Press Ctrl+C to stop.")
        
        # Keep the main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        orchestrator.stop()
        logger.info("Ingestion system stopped")


if __name__ == "__main__":
    main() 