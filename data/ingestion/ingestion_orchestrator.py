import time
import schedule
import threading
from typing import Dict, Any, List, Callable, Optional
from datetime import datetime
from loguru import logger
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from concurrent.futures import ThreadPoolExecutor, as_completed

from config.settings import settings
from data.ingestion.models import AnimalData, VisitorData, WeatherData, SensorData, FeedingData
from data.ingestion.kafka_producer import kafka_producer, schema_registry
from data.ingestion.s3_storage import s3_storage, data_lake
from data.ingestion.data_generator import DataGenerator


# Prometheus metrics
messages_produced = Counter('kafka_messages_produced_total', 'Total messages produced to Kafka', ['topic', 'data_type'])
messages_failed = Counter('kafka_messages_failed_total', 'Total messages failed to produce', ['topic', 'data_type'])
s3_files_saved = Counter('s3_files_saved_total', 'Total files saved to S3', ['data_type', 'layer'])
ingestion_duration = Histogram('ingestion_duration_seconds', 'Time spent on ingestion', ['data_type'])
data_quality_score = Gauge('data_quality_score', 'Data quality score', ['data_type'])
records_processed = Counter('records_processed_total', 'Total records processed', ['data_type'])


class IngestionOrchestrator:
    """Orchestrates data ingestion from multiple sources to Kafka and S3."""
    
    def __init__(self):
        self.data_sources = {}
        self.running = False
        self.executor = ThreadPoolExecutor(max_workers=5)
        self.metrics = {
            'total_messages_sent': 0,
            'total_files_saved': 0,
            'errors': 0,
            'last_run': None
        }
    
    def register_data_source(self, data_type: str, data_generator: Callable[[], List[Dict[str, Any]]]):
        """Register a data source with its generator function."""
        self.data_sources[data_type] = data_generator
        logger.info(f"Registered data source: {data_type}")
    
    def ingest_data(self, data_type: str) -> bool:
        """Ingest data for a specific type."""
        try:
            if data_type not in self.data_sources:
                logger.error(f"Data source {data_type} not registered")
                return False
            
            # Generate data
            data = self.data_sources[data_type]()
            if not data:
                logger.warning(f"No data generated for {data_type}")
                return False
            
            # Validate data
            if not self._validate_data(data_type, data):
                logger.error(f"Data validation failed for {data_type}")
                self.metrics['errors'] += 1
                return False
            
            # Send to Kafka
            kafka_success = kafka_producer.produce_batch(data_type, data)
            if kafka_success > 0:
                self.metrics['total_messages_sent'] += kafka_success
            
            # Save to S3 raw layer
            s3_key = s3_storage.save_raw_data(data_type, data)
            if s3_key:
                self.metrics['total_files_saved'] += 1
            
            logger.info(f"Successfully ingested {len(data)} records for {data_type}")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting data for {data_type}: {e}")
            self.metrics['errors'] += 1
            return False
    
    def _validate_data(self, data_type: str, data: List[Dict[str, Any]]) -> bool:
        """Validate data against schema."""
        try:
            for record in data:
                if not schema_registry.validate_message(data_type, record):
                    return False
            return True
        except Exception as e:
            logger.error(f"Data validation error: {e}")
            return False
    
    def ingest_all_sources(self):
        """Ingest data from all registered sources."""
        logger.info("Starting ingestion for all data sources")
        self.metrics['last_run'] = datetime.utcnow()
        
        # Use ThreadPoolExecutor for parallel ingestion
        futures = []
        for data_type in self.data_sources.keys():
            future = self.executor.submit(self.ingest_data, data_type)
            futures.append((data_type, future))
        
        # Wait for all ingestions to complete
        results = {}
        for data_type, future in futures:
            try:
                success = future.result(timeout=300)  # 5 minute timeout
                results[data_type] = success
            except Exception as e:
                logger.error(f"Error in ingestion task for {data_type}: {e}")
                results[data_type] = False
                self.metrics['errors'] += 1
        
        # Log results
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        logger.info(f"Ingestion completed: {successful}/{total} sources successful")
        
        return results
    
    def start_scheduled_ingestion(self):
        """Start scheduled ingestion jobs."""
        # Schedule different data types at different intervals
        schedule.every(30).seconds.do(self.ingest_data, 'weather')  # Weather every 30 seconds
        schedule.every(1).minutes.do(self.ingest_data, 'sensors')   # Sensors every minute
        schedule.every(2).minutes.do(self.ingest_data, 'animals')   # Animals every 2 minutes
        schedule.every(5).minutes.do(self.ingest_data, 'visitors')  # Visitors every 5 minutes
        schedule.every(10).minutes.do(self.ingest_data, 'feeding')  # Feeding every 10 minutes
        
        # Full ingestion every hour
        schedule.every().hour.do(self.ingest_all_sources)
        
        logger.info("Scheduled ingestion jobs started")
        
        while self.running:
            schedule.run_pending()
            time.sleep(1)
    
    def start(self):
        """Start the ingestion orchestrator."""
        self.running = True
        logger.info("Starting ingestion orchestrator")
        
        # Start scheduled ingestion in a separate thread
        scheduler_thread = threading.Thread(target=self.start_scheduled_ingestion)
        scheduler_thread.daemon = True
        scheduler_thread.start()
        
        logger.info("Ingestion orchestrator started successfully")
    
    def stop(self):
        """Stop the ingestion orchestrator."""
        self.running = False
        self.executor.shutdown(wait=True)
        kafka_producer.close()
        logger.info("Ingestion orchestrator stopped")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics."""
        return {
            **self.metrics,
            'registered_sources': list(self.data_sources.keys()),
            'running': self.running
        }
    
    def run_one_time_ingestion(self, data_type: str = None):
        """Run a one-time ingestion for all sources or a specific source."""
        if data_type:
            logger.info(f"Running one-time ingestion for {data_type}")
            return self.ingest_data(data_type)
        else:
            logger.info("Running one-time ingestion for all sources")
            return self.ingest_all_sources()


def main():
    """Main function to run the ingestion system."""
    # Configure logging
    logger.add("logs/ingestion.log", rotation="1 day", retention="7 days", level="INFO")
    
    # Initialize data generator
    data_generator = DataGenerator()
    
    # Initialize orchestrator
    orchestrator = IngestionOrchestrator()
    
    # Register data sources
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