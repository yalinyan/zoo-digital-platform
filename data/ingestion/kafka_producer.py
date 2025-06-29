import json
import time
from typing import Dict, Any, List, Optional
from datetime import datetime
from confluent_kafka import Producer, KafkaError
from loguru import logger
from config.settings import settings


class KafkaProducerManager:
    """Manages Kafka producers for different data types."""
    
    def __init__(self):
        self.producer_config = {
            'bootstrap.servers': settings.kafka_bootstrap_servers,
            'security.protocol': settings.kafka_security_protocol,
            'client.id': 'zoo-ingestion-producer',
            'acks': 'all',
            'retries': 3,
            'batch.size': 16384,
            'linger.ms': 10,
            'buffer.memory': 33554432,
            'compression.type': 'snappy'
        }
        
        # Add SASL authentication if configured
        if settings.kafka_sasl_mechanism:
            self.producer_config.update({
                'sasl.mechanism': settings.kafka_sasl_mechanism,
                'sasl.username': settings.kafka_sasl_username,
                'sasl.password': settings.kafka_sasl_password
            })
        
        self.producer = Producer(self.producer_config)
        self.topic_mapping = {
            'animals': settings.animal_data_topic,
            'visitors': settings.visitor_data_topic,
            'weather': settings.weather_data_topic,
            'sensors': settings.sensor_data_topic,
            'feeding': settings.feeding_data_topic
        }
    
    def delivery_report(self, err, msg):
        """Callback for message delivery reports."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    
    def produce_message(self, topic: str, key: str, value: Dict[str, Any], 
                       headers: Optional[List] = None) -> bool:
        """Produce a single message to Kafka."""
        try:
            # Serialize the message
            message_value = json.dumps(value, default=str)
            
            # Produce the message
            self.producer.produce(
                topic=topic,
                key=key.encode('utf-8'),
                value=message_value.encode('utf-8'),
                headers=headers,
                callback=self.delivery_report
            )
            
            # Trigger any available delivery reports
            self.producer.poll(0)
            return True
            
        except Exception as e:
            logger.error(f"Error producing message to {topic}: {e}")
            return False
    
    def produce_batch(self, data_type: str, messages: List[Dict[str, Any]]) -> int:
        """Produce a batch of messages to the appropriate topic."""
        topic = self.topic_mapping.get(data_type)
        if not topic:
            logger.error(f"Unknown data type: {data_type}")
            return 0
        
        success_count = 0
        
        for message in messages:
            # Generate a key based on the message content
            if data_type == 'animals':
                key = message.get('animal_id', str(time.time()))
            elif data_type == 'visitors':
                key = message.get('visitor_id', str(time.time()))
            elif data_type == 'weather':
                key = message.get('location', 'zoo_central')
            elif data_type == 'sensors':
                key = message.get('sensor_id', str(time.time()))
            elif data_type == 'feeding':
                key = message.get('feeding_id', str(time.time()))
            else:
                key = str(time.time())
            
            # Add metadata headers
            headers = [
                ('data_type', data_type.encode('utf-8')),
                ('timestamp', str(datetime.utcnow()).encode('utf-8')),
                ('version', '1.0'.encode('utf-8'))
            ]
            
            if self.produce_message(topic, key, message, headers):
                success_count += 1
        
        # Wait for all messages to be delivered
        self.producer.flush()
        
        logger.info(f"Produced {success_count}/{len(messages)} messages to {topic}")
        return success_count
    
    def produce_animal_data(self, animal_data: List[Dict[str, Any]]) -> int:
        """Produce animal data to Kafka."""
        return self.produce_batch('animals', animal_data)
    
    def produce_visitor_data(self, visitor_data: List[Dict[str, Any]]) -> int:
        """Produce visitor data to Kafka."""
        return self.produce_batch('visitors', visitor_data)
    
    def produce_weather_data(self, weather_data: List[Dict[str, Any]]) -> int:
        """Produce weather data to Kafka."""
        return self.produce_batch('weather', weather_data)
    
    def produce_sensor_data(self, sensor_data: List[Dict[str, Any]]) -> int:
        """Produce sensor data to Kafka."""
        return self.produce_batch('sensors', sensor_data)
    
    def produce_feeding_data(self, feeding_data: List[Dict[str, Any]]) -> int:
        """Produce feeding data to Kafka."""
        return self.produce_batch('feeding', feeding_data)
    
    def close(self):
        """Close the producer."""
        self.producer.flush()
        logger.info("Kafka producer closed")


class KafkaSchemaRegistry:
    """Manages Avro schemas for data validation."""
    
    def __init__(self):
        self.schemas = {}
        self._load_schemas()
    
    def _load_schemas(self):
        """Load predefined schemas for different data types."""
        self.schemas = {
            'animals': {
                "type": "record",
                "name": "AnimalData",
                "fields": [
                    {"name": "animal_id", "type": "string"},
                    {"name": "name", "type": "string"},
                    {"name": "species", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "weight", "type": "double"},
                    {"name": "status", "type": "string"},
                    {"name": "enclosure_id", "type": "string"},
                    {"name": "timestamp", "type": "string"}
                ]
            },
            'visitors': {
                "type": "record",
                "name": "VisitorData",
                "fields": [
                    {"name": "visitor_id", "type": "string"},
                    {"name": "ticket_type", "type": "string"},
                    {"name": "entry_time", "type": "string"},
                    {"name": "exit_time", "type": ["null", "string"]},
                    {"name": "age_group", "type": "string"},
                    {"name": "group_size", "type": "int"},
                    {"name": "total_spent", "type": "double"},
                    {"name": "timestamp", "type": "string"}
                ]
            }
        }
    
    def get_schema(self, data_type: str) -> Optional[Dict]:
        """Get schema for a data type."""
        return self.schemas.get(data_type)
    
    def validate_message(self, data_type: str, message: Dict[str, Any]) -> bool:
        """Validate message against schema."""
        schema = self.get_schema(data_type)
        if not schema:
            return True  # No schema defined, assume valid
        
        try:
            # Basic validation - in production, use proper Avro validation
            required_fields = [field['name'] for field in schema['fields'] 
                             if 'null' not in field['type']]
            
            for field in required_fields:
                if field not in message:
                    logger.error(f"Missing required field: {field}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Schema validation error: {e}")
            return False


# Global instances
kafka_producer = KafkaProducerManager()
schema_registry = KafkaSchemaRegistry() 