from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import re
import pandas as pd
from loguru import logger
from data.ingestion.models import (
    AnimalSpecies, AnimalStatus, WeatherCondition,
    AnimalData, VisitorData, WeatherData, SensorData, FeedingData
)


class DataValidator:
    """Data validation and cleaning for silver layer processing."""
    
    def __init__(self):
        self.validation_errors = []
        self.validation_warnings = []
        self.cleaning_rules = self._initialize_cleaning_rules()
    
    def _initialize_cleaning_rules(self) -> Dict[str, Dict]:
        """Initialize data cleaning rules for different data types."""
        return {
            'animals': {
                'name': {
                    'max_length': 100,
                    'pattern': r'^[a-zA-Z\s\-\.]+$',
                    'required': True
                },
                'species': {
                    'valid_values': [s.value for s in AnimalSpecies],
                    'required': True
                },
                'age': {
                    'min_value': 0,
                    'max_value': 100,
                    'required': True
                },
                'weight': {
                    'min_value': 0.1,
                    'max_value': 10000,
                    'required': True
                },
                'temperature': {
                    'min_value': 20,
                    'max_value': 50,
                    'required': False
                },
                'heart_rate': {
                    'min_value': 20,
                    'max_value': 300,
                    'required': False
                }
            },
            'visitors': {
                'ticket_type': {
                    'valid_values': ['adult', 'child', 'senior', 'family', 'group', 'vip'],
                    'required': True
                },
                'age_group': {
                    'valid_values': ['child', 'teen', 'adult', 'senior'],
                    'required': True
                },
                'group_size': {
                    'min_value': 1,
                    'max_value': 50,
                    'required': True
                },
                'total_spent': {
                    'min_value': 0,
                    'max_value': 10000,
                    'required': True
                },
                'satisfaction_rating': {
                    'min_value': 1,
                    'max_value': 5,
                    'required': False
                }
            },
            'weather': {
                'temperature': {
                    'min_value': -50,
                    'max_value': 60,
                    'required': True
                },
                'humidity': {
                    'min_value': 0,
                    'max_value': 100,
                    'required': True
                },
                'pressure': {
                    'min_value': 800,
                    'max_value': 1200,
                    'required': True
                },
                'wind_speed': {
                    'min_value': 0,
                    'max_value': 200,
                    'required': True
                },
                'condition': {
                    'valid_values': [c.value for c in WeatherCondition],
                    'required': True
                }
            },
            'sensors': {
                'sensor_type': {
                    'valid_values': ['temperature', 'humidity', 'air_quality', 'water_ph', 'light', 'noise'],
                    'required': True
                },
                'temperature': {
                    'min_value': -20,
                    'max_value': 60,
                    'required': False
                },
                'humidity': {
                    'min_value': 0,
                    'max_value': 100,
                    'required': False
                },
                'air_quality': {
                    'min_value': 0,
                    'max_value': 500,
                    'required': False
                },
                'battery_level': {
                    'min_value': 0,
                    'max_value': 100,
                    'required': False
                }
            },
            'feeding': {
                'food_type': {
                    'max_length': 50,
                    'pattern': r'^[a-zA-Z\s\-]+$',
                    'required': True
                },
                'quantity': {
                    'min_value': 0.01,
                    'max_value': 100,
                    'required': True
                },
                'consumed_amount': {
                    'min_value': 0,
                    'max_value': 100,
                    'required': False
                }
            }
        }
    
    def validate_and_clean_record(self, data_type: str, record: Dict[str, Any]) -> Tuple[Dict[str, Any], float]:
        """
        Validate and clean a single record.
        
        Args:
            data_type: Type of data (animals, visitors, weather, sensors, feeding)
            record: Raw data record
            
        Returns:
            Tuple of (cleaned_record, quality_score)
        """
        self.validation_errors = []
        self.validation_warnings = []
        
        # Deep copy the record to avoid modifying original
        cleaned_record = record.copy()
        
        # Apply data type specific validation and cleaning
        if data_type == 'animals':
            cleaned_record = self._validate_and_clean_animal(cleaned_record)
        elif data_type == 'visitors':
            cleaned_record = self._validate_and_clean_visitor(cleaned_record)
        elif data_type == 'weather':
            cleaned_record = self._validate_and_clean_weather(cleaned_record)
        elif data_type == 'sensors':
            cleaned_record = self._validate_and_clean_sensor(cleaned_record)
        elif data_type == 'feeding':
            cleaned_record = self._validate_and_clean_feeding(cleaned_record)
        else:
            self.validation_errors.append(f"Unknown data type: {data_type}")
            return cleaned_record, 0.0
        
        # Calculate quality score
        quality_score = self._calculate_quality_score(record, cleaned_record)
        
        return cleaned_record, quality_score
    
    def _validate_and_clean_animal(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and clean animal data."""
        cleaned = record.copy()
        
        # Clean and validate name
        if 'name' in cleaned:
            cleaned['name'] = self._clean_string(cleaned['name'], max_length=100)
            if not re.match(r'^[a-zA-Z\s\-\.]+$', cleaned['name']):
                self.validation_errors.append(f"Invalid animal name: {cleaned['name']}")
        
        # Validate species
        if 'species' in cleaned:
            if cleaned['species'] not in [s.value for s in AnimalSpecies]:
                self.validation_errors.append(f"Invalid species: {cleaned['species']}")
        
        # Validate and clean numeric fields
        cleaned['age'] = self._validate_numeric(cleaned.get('age'), 'age', 0, 100)
        cleaned['weight'] = self._validate_numeric(cleaned.get('weight'), 'weight', 0.1, 10000)
        cleaned['temperature'] = self._validate_numeric(cleaned.get('temperature'), 'temperature', 20, 50)
        cleaned['heart_rate'] = self._validate_numeric(cleaned.get('heart_rate'), 'heart_rate', 20, 300)
        
        # Validate status
        if 'status' in cleaned:
            if cleaned['status'] not in [s.value for s in AnimalStatus]:
                self.validation_errors.append(f"Invalid status: {cleaned['status']}")
        
        # Clean timestamps
        cleaned['last_feeding'] = self._clean_timestamp(cleaned.get('last_feeding'))
        cleaned['timestamp'] = self._clean_timestamp(cleaned.get('timestamp'))
        
        return cleaned
    
    def _validate_and_clean_visitor(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and clean visitor data."""
        cleaned = record.copy()
        
        # Validate ticket type
        valid_ticket_types = ['adult', 'child', 'senior', 'family', 'group', 'vip']
        if 'ticket_type' in cleaned and cleaned['ticket_type'] not in valid_ticket_types:
            self.validation_errors.append(f"Invalid ticket type: {cleaned['ticket_type']}")
        
        # Validate age group
        valid_age_groups = ['child', 'teen', 'adult', 'senior']
        if 'age_group' in cleaned and cleaned['age_group'] not in valid_age_groups:
            self.validation_errors.append(f"Invalid age group: {cleaned['age_group']}")
        
        # Validate and clean numeric fields
        cleaned['group_size'] = self._validate_numeric(cleaned.get('group_size'), 'group_size', 1, 50)
        cleaned['total_spent'] = self._validate_numeric(cleaned.get('total_spent'), 'total_spent', 0, 10000)
        cleaned['satisfaction_rating'] = self._validate_numeric(cleaned.get('satisfaction_rating'), 'satisfaction_rating', 1, 5)
        
        # Clean timestamps
        cleaned['entry_time'] = self._clean_timestamp(cleaned.get('entry_time'))
        cleaned['exit_time'] = self._clean_timestamp(cleaned.get('exit_time'))
        cleaned['timestamp'] = self._clean_timestamp(cleaned.get('timestamp'))
        
        # Calculate visit duration if both times are available
        if cleaned.get('entry_time') and cleaned.get('exit_time'):
            duration = (cleaned['exit_time'] - cleaned['entry_time']).total_seconds() / 60
            if duration > 0:
                cleaned['visit_duration_minutes'] = int(duration)
            else:
                self.validation_warnings.append("Exit time is before entry time")
        
        # Clean visited enclosures list
        if 'visited_enclosures' in cleaned and isinstance(cleaned['visited_enclosures'], list):
            cleaned['visited_enclosures'] = [str(e).strip() for e in cleaned['visited_enclosures'] if e]
        
        return cleaned
    
    def _validate_and_clean_weather(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and clean weather data."""
        cleaned = record.copy()
        
        # Validate and clean numeric fields
        cleaned['temperature'] = self._validate_numeric(cleaned.get('temperature'), 'temperature', -50, 60)
        cleaned['humidity'] = self._validate_numeric(cleaned.get('humidity'), 'humidity', 0, 100)
        cleaned['pressure'] = self._validate_numeric(cleaned.get('pressure'), 'pressure', 800, 1200)
        cleaned['wind_speed'] = self._validate_numeric(cleaned.get('wind_speed'), 'wind_speed', 0, 200)
        cleaned['wind_direction'] = self._validate_numeric(cleaned.get('wind_direction'), 'wind_direction', 0, 360)
        cleaned['visibility'] = self._validate_numeric(cleaned.get('visibility'), 'visibility', 0, 50)
        cleaned['uv_index'] = self._validate_numeric(cleaned.get('uv_index'), 'uv_index', 0, 15)
        
        # Validate condition
        if 'condition' in cleaned:
            if cleaned['condition'] not in [c.value for c in WeatherCondition]:
                self.validation_errors.append(f"Invalid weather condition: {cleaned['condition']}")
        
        # Clean timestamp
        cleaned['timestamp'] = self._clean_timestamp(cleaned.get('timestamp'))
        
        return cleaned
    
    def _validate_and_clean_sensor(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and clean sensor data."""
        cleaned = record.copy()
        
        # Validate sensor type
        valid_sensor_types = ['temperature', 'humidity', 'air_quality', 'water_ph', 'light', 'noise']
        if 'sensor_type' in cleaned and cleaned['sensor_type'] not in valid_sensor_types:
            self.validation_errors.append(f"Invalid sensor type: {cleaned['sensor_type']}")
        
        # Validate and clean numeric fields based on sensor type
        sensor_type = cleaned.get('sensor_type')
        if sensor_type == 'temperature':
            cleaned['temperature'] = self._validate_numeric(cleaned.get('temperature'), 'temperature', -20, 60)
        elif sensor_type == 'humidity':
            cleaned['humidity'] = self._validate_numeric(cleaned.get('humidity'), 'humidity', 0, 100)
        elif sensor_type == 'air_quality':
            cleaned['air_quality'] = self._validate_numeric(cleaned.get('air_quality'), 'air_quality', 0, 500)
        elif sensor_type == 'water_ph':
            cleaned['water_ph'] = self._validate_numeric(cleaned.get('water_ph'), 'water_ph', 0, 14)
            cleaned['water_temperature'] = self._validate_numeric(cleaned.get('water_temperature'), 'water_temperature', -10, 40)
        elif sensor_type == 'light':
            cleaned['light_intensity'] = self._validate_numeric(cleaned.get('light_intensity'), 'light_intensity', 0, 100000)
        elif sensor_type == 'noise':
            cleaned['noise_level'] = self._validate_numeric(cleaned.get('noise_level'), 'noise_level', 0, 200)
        
        # Validate battery level
        cleaned['battery_level'] = self._validate_numeric(cleaned.get('battery_level'), 'battery_level', 0, 100)
        
        # Clean timestamp
        cleaned['timestamp'] = self._clean_timestamp(cleaned.get('timestamp'))
        
        return cleaned
    
    def _validate_and_clean_feeding(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and clean feeding data."""
        cleaned = record.copy()
        
        # Clean food type
        if 'food_type' in cleaned:
            cleaned['food_type'] = self._clean_string(cleaned['food_type'], max_length=50)
            if not re.match(r'^[a-zA-Z\s\-]+$', cleaned['food_type']):
                self.validation_errors.append(f"Invalid food type: {cleaned['food_type']}")
        
        # Validate and clean numeric fields
        cleaned['quantity'] = self._validate_numeric(cleaned.get('quantity'), 'quantity', 0.01, 100)
        cleaned['consumed_amount'] = self._validate_numeric(cleaned.get('consumed_amount'), 'consumed_amount', 0, 100)
        
        # Calculate consumption percentage
        if cleaned.get('quantity') and cleaned.get('consumed_amount'):
            percentage = (cleaned['consumed_amount'] / cleaned['quantity']) * 100
            if 0 <= percentage <= 100:
                cleaned['consumption_percentage'] = round(percentage, 2)
        
        # Clean timestamps
        cleaned['feeding_time'] = self._clean_timestamp(cleaned.get('feeding_time'))
        cleaned['timestamp'] = self._clean_timestamp(cleaned.get('timestamp'))
        
        return cleaned
    
    def _clean_string(self, value: Any, max_length: int = None) -> str:
        """Clean string values."""
        if value is None:
            return None
        
        cleaned = str(value).strip()
        if max_length and len(cleaned) > max_length:
            cleaned = cleaned[:max_length]
            self.validation_warnings.append(f"String truncated to {max_length} characters")
        
        return cleaned if cleaned else None
    
    def _validate_numeric(self, value: Any, field_name: str, min_val: float, max_val: float) -> Optional[float]:
        """Validate and clean numeric values."""
        if value is None:
            return None
        
        try:
            num_val = float(value)
            if min_val <= num_val <= max_val:
                return num_val
            else:
                self.validation_errors.append(f"{field_name} value {num_val} is outside valid range [{min_val}, {max_val}]")
                return None
        except (ValueError, TypeError):
            self.validation_errors.append(f"Invalid numeric value for {field_name}: {value}")
            return None
    
    def _clean_timestamp(self, value: Any) -> Optional[datetime]:
        """Clean and validate timestamp values."""
        if value is None:
            return None
        
        if isinstance(value, datetime):
            return value
        
        if isinstance(value, str):
            try:
                # Try common timestamp formats
                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%d']:
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
                self.validation_errors.append(f"Invalid timestamp format: {value}")
                return None
            except Exception:
                self.validation_errors.append(f"Invalid timestamp: {value}")
                return None
        
        return None
    
    def _calculate_quality_score(self, original: Dict[str, Any], cleaned: Dict[str, Any]) -> float:
        """Calculate data quality score based on validation results."""
        if not original:
            return 0.0
        
        total_fields = len(original)
        if total_fields == 0:
            return 1.0
        
        # Start with base score
        score = 1.0
        
        # Deduct for validation errors
        error_penalty = len(self.validation_errors) * 0.1
        score -= min(error_penalty, 0.5)  # Max 50% penalty for errors
        
        # Deduct for warnings
        warning_penalty = len(self.validation_warnings) * 0.05
        score -= min(warning_penalty, 0.2)  # Max 20% penalty for warnings
        
        # Deduct for missing required fields
        missing_fields = sum(1 for v in cleaned.values() if v is None)
        missing_penalty = (missing_fields / total_fields) * 0.3
        score -= missing_penalty
        
        return max(0.0, min(1.0, score))
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """Get validation summary."""
        return {
            'errors': self.validation_errors,
            'warnings': self.validation_warnings,
            'error_count': len(self.validation_errors),
            'warning_count': len(self.validation_warnings)
        }


class SilverLayerProcessor:
    """Main processor for silver layer data transformation."""
    
    def __init__(self):
        self.validator = DataValidator()
        self.batch_id = None
    
    def process_batch(self, data_type: str, records: List[Dict[str, Any]], 
                     batch_id: str = None) -> Dict[str, Any]:
        """
        Process a batch of records for silver layer.
        
        Args:
            data_type: Type of data being processed
            records: List of raw records
            batch_id: Optional batch identifier
            
        Returns:
            Processing results summary
        """
        self.batch_id = batch_id or f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        start_time = datetime.utcnow()
        total_records = len(records)
        valid_records = []
        invalid_records = []
        
        logger.info(f"Processing {total_records} {data_type} records for batch {self.batch_id}")
        
        for i, record in enumerate(records):
            try:
                cleaned_record, quality_score = self.validator.validate_and_clean_record(data_type, record)
                
                # Add metadata
                cleaned_record['data_quality_score'] = quality_score
                cleaned_record['ingestion_batch_id'] = self.batch_id
                cleaned_record['source_file'] = record.get('source_file', 'unknown')
                
                if quality_score >= 0.7:  # Quality threshold
                    valid_records.append(cleaned_record)
                else:
                    invalid_records.append({
                        'original_record': record,
                        'cleaned_record': cleaned_record,
                        'quality_score': quality_score,
                        'validation_summary': self.validator.get_validation_summary()
                    })
                
                if (i + 1) % 100 == 0:
                    logger.info(f"Processed {i + 1}/{total_records} records")
                    
            except Exception as e:
                logger.error(f"Error processing record {i}: {e}")
                invalid_records.append({
                    'original_record': record,
                    'error': str(e)
                })
        
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        quality_score = len(valid_records) / total_records if total_records > 0 else 0.0
        
        results = {
            'batch_id': self.batch_id,
            'data_type': data_type,
            'total_records': total_records,
            'valid_records': len(valid_records),
            'invalid_records': len(invalid_records),
            'quality_score': quality_score,
            'processing_time_seconds': processing_time,
            'processed_at': datetime.utcnow(),
            'valid_records': valid_records,
            'invalid_records': invalid_records
        }
        
        logger.info(f"Batch {self.batch_id} completed: {len(valid_records)}/{total_records} valid records "
                   f"(quality: {quality_score:.2f}, time: {processing_time:.2f}s)")
        
        return results 