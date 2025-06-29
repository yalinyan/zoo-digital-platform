import json
import uuid
from typing import Dict, Any, Optional, Callable
from datetime import datetime
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from loguru import logger
from config.settings import settings


class KafkaClient:
    def __init__(self):
        self.producer_config = {
            'bootstrap.servers': settings.kafka_bootstrap_servers,
            'client.id': f'zoo-ingestion-{uuid.uuid4().hex[:8]}',
            'acks': 'all',
            'retries': 3,
            'batch.size': 16384,
            'linger.ms': 10,
            'buffer.memory': 33554432,
        }
        
        self.consumer_config = {
            'bootstrap.servers': settings.kafka_bootstrap_servers,
            'group.id': f'zoo-consumer-{uuid.uuid4().hex[:8]}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000,
        }
        
        # Add security configuration if provided
        if settings.kafka_security_protocol != "PLAINTEXT":
            self.producer_config.update({
                'security.protocol': settings.kafka_security_protocol,
                'sasl.mechanism': settings.kafka_sasl_mechanism,
                'sasl.username': settings.kafka_sasl_username,
                'sasl.password': settings.kafka_sasl_password,
            })
            self.consumer_config.update({
                'security.protocol': settings.kafka_security_protocol,
                'sasl.mechanism': settings.kafka_sasl_mechanism,
                'sasl.username': settings.kafka_sasl_username,
                'sasl.password': settings.kafka_sasl_password,
            })
        
        self.producer = None
        self.consumer = None
    
    def get_producer(self) -> Producer:
        """Get or create a Kafka producer instance."""
        if self.producer is None:
            try:
                self.producer = Producer(self.producer_config)
                logger.info("Kafka producer initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Kafka producer: {e}")
                raise
        return self.producer
    
    def get_consumer(self) -> Consumer:
        """Get or create a Kafka consumer instance."""
        if self.consumer is None:
            try:
                self.consumer = Consumer(self.consumer_config)
                logger.info("Kafka consumer initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Kafka consumer: {e}")
                raise
        return self.consumer
    
    def delivery_report(self, err, msg):
        """Delivery report callback for producer."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    
    def produce_message(self, topic: str, key: str, value: Dict[str, Any], 
                       headers: Optional[Dict[str, str]] = None) -> bool:
        """
        Produce a message to Kafka topic.
        
        Args:
            topic: Kafka topic name
            key: Message key
            value: Message value (will be JSON serialized)
            headers: Optional message headers
            
        Returns:
            bool: True if message was queued successfully, False otherwise
        """
        try:
            producer = self.get_producer()
            
            # Serialize value to JSON
            json_value = json.dumps(value, default=str)
            
            # Prepare headers
            kafka_headers = []
            if headers:
                for k, v in headers.items():
                    kafka_headers.append((k, v.encode('utf-8')))
            
            # Add timestamp header
            kafka_headers.append(('timestamp', str(datetime.utcnow().isoformat()).encode('utf-8')))
            
            # Produce message
            producer.produce(
                topic=topic,
                key=key.encode('utf-8'),
                value=json_value.encode('utf-8'),
                headers=kafka_headers,
                callback=self.delivery_report
            )
            
            # Trigger any available delivery reports
            producer.poll(0)
            
            logger.debug(f"Message queued for topic {topic} with key {key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to produce message to topic {topic}: {e}")
            return False
    
    def produce_batch(self, topic: str, messages: list) -> Dict[str, int]:
        """
        Produce a batch of messages to Kafka topic.
        
        Args:
            topic: Kafka topic name
            messages: List of dicts with 'key', 'value', and optional 'headers'
            
        Returns:
            Dict with success and failure counts
        """
        producer = self.get_producer()
        success_count = 0
        failure_count = 0
        
        for msg in messages:
            try:
                key = msg.get('key', str(uuid.uuid4()))
                value = msg['value']
                headers = msg.get('headers', {})
                
                if self.produce_message(topic, key, value, headers):
                    success_count += 1
                else:
                    failure_count += 1
                    
            except Exception as e:
                logger.error(f"Failed to process message in batch: {e}")
                failure_count += 1
        
        # Wait for all messages to be delivered
        producer.flush()
        
        logger.info(f"Batch production completed: {success_count} success, {failure_count} failures")
        return {'success': success_count, 'failure': failure_count}
    
    def consume_messages(self, topics: list, callback: Callable, 
                        timeout: float = 1.0, max_messages: Optional[int] = None) -> int:
        """
        Consume messages from Kafka topics.
        
        Args:
            topics: List of topics to subscribe to
            callback: Function to call for each message (signature: callback(topic, key, value, headers))
            timeout: Poll timeout in seconds
            max_messages: Maximum number of messages to consume (None for unlimited)
            
        Returns:
            int: Number of messages consumed
        """
        consumer = self.get_consumer()
        message_count = 0
        
        try:
            consumer.subscribe(topics)
            logger.info(f"Subscribed to topics: {topics}")
            
            while True:
                if max_messages and message_count >= max_messages:
                    break
                
                msg = consumer.poll(timeout)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                try:
                    # Parse message
                    topic = msg.topic()
                    key = msg.key().decode('utf-8') if msg.key() else None
                    value = json.loads(msg.value().decode('utf-8')) if msg.value() else None
                    
                    # Parse headers
                    headers = {}
                    if msg.headers():
                        for header_key, header_value in msg.headers():
                            headers[header_key.decode('utf-8')] = header_value.decode('utf-8')
                    
                    # Call callback
                    callback(topic, key, value, headers)
                    message_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue
                    
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            consumer.close()
            logger.info(f"Consumer closed. Total messages consumed: {message_count}")
        
        return message_count
    
    def close(self):
        """Close producer and consumer connections."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")
        
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")


# Global Kafka client instance
kafka_client = KafkaClient() 