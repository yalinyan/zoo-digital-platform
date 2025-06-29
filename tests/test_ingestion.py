import pytest
import tempfile
import os
from unittest.mock import Mock, patch
from datetime import datetime

from data.ingestion.models import AnimalData, VisitorData, WeatherData, SensorData, FeedingData
from data.ingestion.data_generator import DataGenerator
from data.ingestion.kafka_producer import KafkaProducerManager, KafkaSchemaRegistry


class TestDataModels:
    """Test data model validation."""
    
    def test_animal_data_validation(self):
        """Test animal data model validation."""
        animal_data = {
            "animal_id": "A12345678",
            "name": "Simba",
            "species": "lion",
            "age": 5,
            "weight": 180.5,
            "status": "healthy",
            "enclosure_id": "E001",
            "temperature": 38.5,
            "heart_rate": 80,
            "timestamp": datetime.utcnow()
        }
        
        animal = AnimalData(**animal_data)
        assert animal.name == "Simba"
        assert animal.species.value == "lion"
        assert animal.status.value == "healthy"
    
    def test_visitor_data_validation(self):
        """Test visitor data model validation."""
        visitor_data = {
            "visitor_id": "V12345678",
            "ticket_type": "adult",
            "entry_time": datetime.utcnow(),
            "age_group": "adult",
            "group_size": 2,
            "total_spent": 50.0,
            "timestamp": datetime.utcnow()
        }
        
        visitor = VisitorData(**visitor_data)
        assert visitor.ticket_type == "adult"
        assert visitor.group_size == 2
        assert visitor.total_spent == 50.0
    
    def test_weather_data_validation(self):
        """Test weather data model validation."""
        weather_data = {
            "location": "Zoo Central",
            "temperature": 25.0,
            "humidity": 60.0,
            "pressure": 1013.25,
            "wind_speed": 5.0,
            "wind_direction": 180.0,
            "condition": "sunny",
            "visibility": 10.0,
            "timestamp": datetime.utcnow()
        }
        
        weather = WeatherData(**weather_data)
        assert weather.location == "Zoo Central"
        assert weather.temperature == 25.0
        assert weather.condition.value == "sunny"


class TestDataGenerator:
    """Test data generator functionality."""
    
    def setup_method(self):
        """Setup test method."""
        self.generator = DataGenerator()
    
    def test_generate_animal_data(self):
        """Test animal data generation."""
        animals = self.generator.generate_animal_data(3)
        
        assert len(animals) == 3
        for animal in animals:
            assert "animal_id" in animal
            assert "name" in animal
            assert "species" in animal
            assert "age" in animal
            assert "weight" in animal
            assert "status" in animal
            assert "enclosure_id" in animal
            assert "timestamp" in animal
    
    def test_generate_visitor_data(self):
        """Test visitor data generation."""
        visitors = self.generator.generate_visitor_data(5)
        
        assert len(visitors) == 5
        for visitor in visitors:
            assert "visitor_id" in visitor
            assert "ticket_type" in visitor
            assert "entry_time" in visitor
            assert "age_group" in visitor
            assert "group_size" in visitor
            assert "total_spent" in visitor
            assert "timestamp" in visitor
    
    def test_generate_weather_data(self):
        """Test weather data generation."""
        weather_records = self.generator.generate_weather_data(2)
        
        assert len(weather_records) == 2
        for weather in weather_records:
            assert "location" in weather
            assert "temperature" in weather
            assert "humidity" in weather
            assert "pressure" in weather
            assert "wind_speed" in weather
            assert "condition" in weather
            assert "timestamp" in weather
    
    def test_generate_sensor_data(self):
        """Test sensor data generation."""
        sensor_records = self.generator.generate_sensor_data(4)
        
        assert len(sensor_records) == 4
        for sensor in sensor_records:
            assert "sensor_id" in sensor
            assert "sensor_type" in sensor
            assert "enclosure_id" in sensor
            assert "timestamp" in sensor
    
    def test_generate_feeding_data(self):
        """Test feeding data generation."""
        feeding_records = self.generator.generate_feeding_data(3)
        
        assert len(feeding_records) == 3
        for feeding in feeding_records:
            assert "feeding_id" in feeding
            assert "animal_id" in feeding
            assert "food_type" in feeding
            assert "quantity" in feeding
            assert "feeding_time" in feeding
            assert "keeper_id" in feeding
            assert "timestamp" in feeding


class TestKafkaSchemaRegistry:
    """Test Kafka schema registry functionality."""
    
    def setup_method(self):
        """Setup test method."""
        self.registry = KafkaSchemaRegistry()
    
    def test_get_schema(self):
        """Test schema retrieval."""
        animal_schema = self.registry.get_schema('animals')
        assert animal_schema is not None
        assert animal_schema['name'] == 'AnimalData'
        
        visitor_schema = self.registry.get_schema('visitors')
        assert visitor_schema is not None
        assert visitor_schema['name'] == 'VisitorData'
    
    def test_validate_message(self):
        """Test message validation."""
        animal_message = {
            "animal_id": "A12345678",
            "name": "Simba",
            "species": "lion",
            "age": 5,
            "weight": 180.5,
            "status": "healthy",
            "enclosure_id": "E001",
            "timestamp": "2024-01-01T12:00:00"
        }
        
        # Should pass validation
        assert self.registry.validate_message('animals', animal_message) is True
        
        # Should fail validation (missing required field)
        invalid_message = animal_message.copy()
        del invalid_message['animal_id']
        assert self.registry.validate_message('animals', invalid_message) is False


class TestKafkaProducer:
    """Test Kafka producer functionality."""
    
    @patch('confluent_kafka.Producer')
    def test_producer_initialization(self, mock_producer):
        """Test Kafka producer initialization."""
        mock_producer_instance = Mock()
        mock_producer.return_value = mock_producer_instance
        
        producer_manager = KafkaProducerManager()
        
        assert producer_manager.producer is not None
        assert 'animals' in producer_manager.topic_mapping
        assert 'visitors' in producer_manager.topic_mapping
        assert 'weather' in producer_manager.topic_mapping
        assert 'sensors' in producer_manager.topic_mapping
        assert 'feeding' in producer_manager.topic_mapping
    
    @patch('confluent_kafka.Producer')
    def test_produce_batch(self, mock_producer):
        """Test batch message production."""
        mock_producer_instance = Mock()
        mock_producer.return_value = mock_producer_instance
        
        producer_manager = KafkaProducerManager()
        
        # Test data
        animal_data = [
            {
                "animal_id": "A12345678",
                "name": "Simba",
                "species": "lion",
                "age": 5,
                "weight": 180.5,
                "status": "healthy",
                "enclosure_id": "E001",
                "timestamp": datetime.utcnow()
            }
        ]
        
        # Mock successful production
        mock_producer_instance.produce.return_value = None
        mock_producer_instance.flush.return_value = None
        
        success_count = producer_manager.produce_animal_data(animal_data)
        
        # Should have called produce and flush
        assert mock_producer_instance.produce.called
        assert mock_producer_instance.flush.called


if __name__ == "__main__":
    pytest.main([__file__]) 