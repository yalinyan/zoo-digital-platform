#!/usr/bin/env python3
"""
Zoo Digital Platform - Data Ingestion Runner

This script runs the data ingestion system for the zoo digital platform.
It can be used to ingest data from multiple sources into Kafka and S3.

Usage:
    python scripts/run_ingestion.py [--data-type TYPE] [--count COUNT] [--continuous]

Examples:
    python scripts/run_ingestion.py --data-type animals --count 10
    python scripts/run_ingestion.py --data-type all --continuous
    python scripts/run_ingestion.py --continuous
"""

import argparse
import sys
import os
import time
from datetime import datetime

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from data.ingestion.ingestion_orchestrator import IngestionOrchestrator
from data.ingestion.data_generator import DataGenerator


def setup_logging():
    """Setup logging configuration."""
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Configure loguru
    logger.remove()  # Remove default handler
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )
    logger.add(
        "logs/ingestion.log",
        rotation="1 day",
        retention="7 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG"
    )


def run_single_ingestion(data_type: str, count: int = 1):
    """Run a single ingestion for a specific data type."""
    logger.info(f"Running single ingestion for {data_type} with {count} records")
    
    # Initialize data generator
    data_generator = DataGenerator()
    
    # Initialize orchestrator
    orchestrator = IngestionOrchestrator()
    
    # Register data sources
    orchestrator.register_data_source('animals', lambda: data_generator.generate_animal_data(count))
    orchestrator.register_data_source('visitors', lambda: data_generator.generate_visitor_data(count))
    orchestrator.register_data_source('weather', lambda: data_generator.generate_weather_data(count))
    orchestrator.register_data_source('sensors', lambda: data_generator.generate_sensor_data(count))
    orchestrator.register_data_source('feeding', lambda: data_generator.generate_feeding_data(count))
    
    try:
        if data_type == 'all':
            # Run ingestion for all data types
            results = orchestrator.run_one_time_ingestion()
            logger.info(f"Ingestion completed: {results}")
        else:
            # Run ingestion for specific data type
            success = orchestrator.run_one_time_ingestion(data_type)
            logger.info(f"Ingestion for {data_type}: {'Success' if success else 'Failed'}")
            
    except Exception as e:
        logger.error(f"Error during ingestion: {e}")
        return False
    
    return True


def run_continuous_ingestion():
    """Run continuous ingestion with scheduled jobs."""
    logger.info("Starting continuous ingestion system")
    
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
        
        logger.info("Continuous ingestion system is running. Press Ctrl+C to stop.")
        
        # Keep the main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        orchestrator.stop()
        logger.info("Continuous ingestion system stopped")


def main():
    """Main function to parse arguments and run ingestion."""
    parser = argparse.ArgumentParser(
        description="Zoo Digital Platform - Data Ingestion Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--data-type',
        choices=['animals', 'visitors', 'weather', 'sensors', 'feeding', 'all'],
        default='all',
        help='Type of data to ingest (default: all)'
    )
    
    parser.add_argument(
        '--count',
        type=int,
        default=1,
        help='Number of records to generate (default: 1)'
    )
    
    parser.add_argument(
        '--continuous',
        action='store_true',
        help='Run continuous ingestion with scheduled jobs'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    if args.verbose:
        logger.add(sys.stderr, level="DEBUG")
    
    logger.info("Starting Zoo Digital Platform Data Ingestion")
    logger.info(f"Data type: {args.data_type}")
    logger.info(f"Count: {args.count}")
    logger.info(f"Continuous mode: {args.continuous}")
    
    try:
        if args.continuous:
            run_continuous_ingestion()
        else:
            success = run_single_ingestion(args.data_type, args.count)
            if not success:
                sys.exit(1)
                
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 