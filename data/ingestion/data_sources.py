import requests
import json
import time
import random
from typing import Dict, List, Any, Optional, Generator
from datetime import datetime, timedelta
from loguru import logger
from config.settings import settings
from data.ingestion.models import (
    AnimalData, VisitorData, WeatherData, SensorData, FeedingData,
    AnimalSpecies, AnimalStatus, WeatherCondition
)


class ZooAPIClient:
    """Client for Zoo Management System API."""
    
    def __init__(self):
        self.base_url = settings.zoo_api_base_url
        self.api_key = settings.zoo_api_key
        self.session = requests.Session()
        
        if self.api_key:
            self.session.headers.update({'Authorization': f'Bearer {self.api_key}'})
        
        self.session.headers.update({'Content-Type': 'application/json'})
    
    def _make_request(self, endpoint: str, method: str = 'GET', 
                     params: Optional[Dict] = None, data: Optional[Dict] = None) -> Optional[Dict]:
        """Make HTTP request to Zoo API."""
        try:
            url = f"{self.base_url}{endpoint}"
            response = self.session.request(method, url, params=params, json=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return None
    
    def get_animals(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Fetch animal data from Zoo API."""
        endpoint = "/api/v1/animals"
        params = {'limit': limit}
        
        data = self._make_request(endpoint, params=params)
        if data:
            return data.get('animals', [])
        return []
    
    def get_animal_health(self, animal_id: str) -> Optional[Dict[str, Any]]:
        """Fetch health data for specific animal."""
        endpoint = f"/api/v1/animals/{animal_id}/health"
        return self._make_request(endpoint)
    
    def get_visitors(self, date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch visitor data from Zoo API."""
        endpoint = "/api/v1/visitors"
        params = {}
        if date:
            params['date'] = date
        
        data = self._make_request(endpoint, params=params)
        if data:
            return data.get('visitors', [])
        return []
    
    def get_feeding_schedule(self) -> List[Dict[str, Any]]:
        """Fetch feeding schedule data."""
        endpoint = "/api/v1/feeding/schedule"
        data = self._make_request(endpoint)
        if data:
            return data.get('feedings', [])
        return []


class WeatherAPIClient:
    """Client for Weather API."""
    
    def __init__(self):
        self.api_key = settings.weather_api_key
        self.base_url = settings.weather_api_url
    
    def get_current_weather(self, location: str) -> Optional[Dict[str, Any]]:
        """Fetch current weather data."""
        if not self.api_key:
            logger.warning("Weather API key not configured")
            return self._generate_mock_weather(location)
        
        try:
            url = f"{self.base_url}/weather"
            params = {
                'q': location,
                'appid': self.api_key,
                'units': 'metric'
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Weather API request failed: {e}")
            return self._generate_mock_weather(location)
    
    def _generate_mock_weather(self, location: str) -> Dict[str, Any]:
        """Generate mock weather data for testing."""
        conditions = ['sunny', 'cloudy', 'rainy', 'windy']
        return {
            'location': location,
            'temperature': random.uniform(15, 35),
            'humidity': random.uniform(30, 80),
            'pressure': random.uniform(1000, 1020),
            'wind_speed': random.uniform(0, 20),
            'wind_direction': random.uniform(0, 360),
            'condition': random.choice(conditions),
            'visibility': random.uniform(5, 20),
            'uv_index': random.uniform(0, 10)
        }


class SensorDataGenerator:
    """Generate mock sensor data for IoT devices."""
    
    def __init__(self):
        self.sensor_types = ['temperature', 'humidity', 'air_quality', 'water_ph', 'light', 'noise']
        self.enclosures = ['lion_den', 'elephant_habitat', 'penguin_pool', 'gorilla_forest', 'giraffe_plains']
    
    def generate_sensor_data(self, sensor_id: str, sensor_type: str, 
                           enclosure_id: str) -> Dict[str, Any]:
        """Generate sensor reading based on type."""
        base_data = {
            'sensor_id': sensor_id,
            'sensor_type': sensor_type,
            'enclosure_id': enclosure_id,
            'timestamp': datetime.utcnow()
        }
        
        if sensor_type == 'temperature':
            base_data['temperature'] = random.uniform(18, 28)
        elif sensor_type == 'humidity':
            base_data['humidity'] = random.uniform(40, 70)
        elif sensor_type == 'air_quality':
            base_data['air_quality'] = random.uniform(0, 150)
        elif sensor_type == 'water_ph':
            base_data['water_ph'] = random.uniform(6.5, 8.5)
        elif sensor_type == 'light':
            base_data['light_intensity'] = random.uniform(0, 1000)
        elif sensor_type == 'noise':
            base_data['noise_level'] = random.uniform(30, 80)
        
        base_data['battery_level'] = random.uniform(20, 100)
        return base_data


class MockDataGenerator:
    """Generate mock data for testing and development."""
    
    def __init__(self):
        self.animal_names = [
            'Leo', 'Simba', 'Nala', 'Mufasa', 'Tigger', 'Stripes', 'Dumbo', 'Jumbo',
            'Gerry', 'Melman', 'Happy', 'Waddles', 'Flipper', 'Koko', 'Silverback',
            'Ziggy', 'Rex', 'Panda', 'Bamboo', 'Joey', 'Bouncy'
        ]
        
        self.species_list = list(AnimalSpecies)
        self.status_list = list(AnimalStatus)
        self.enclosures = ['lion_den', 'tiger_habitat', 'elephant_plains', 'giraffe_heights',
                          'penguin_pool', 'gorilla_forest', 'zebra_plains', 'rhino_sanctuary',
                          'panda_garden', 'kangaroo_outback']
        
        self.ticket_types = ['adult', 'child', 'senior', 'family', 'vip']
        self.age_groups = ['child', 'teen', 'adult', 'senior']
    
    def generate_animal_data(self, count: int = 1) -> List[Dict[str, Any]]:
        """Generate mock animal data."""
        animals = []
        
        for i in range(count):
            animal = {
                'animal_id': f"ANM_{random.randint(1000, 9999)}",
                'name': random.choice(self.animal_names),
                'species': random.choice(self.species_list).value,
                'age': random.randint(1, 25),
                'weight': random.uniform(50, 5000),
                'status': random.choice(self.status_list).value,
                'enclosure_id': random.choice(self.enclosures),
                'last_feeding': datetime.utcnow() - timedelta(hours=random.randint(1, 12)),
                'temperature': random.uniform(36, 40),
                'heart_rate': random.randint(60, 120),
                'timestamp': datetime.utcnow()
            }
            animals.append(animal)
        
        return animals
    
    def generate_visitor_data(self, count: int = 1) -> List[Dict[str, Any]]:
        """Generate mock visitor data."""
        visitors = []
        
        for i in range(count):
            entry_time = datetime.utcnow() - timedelta(hours=random.randint(1, 8))
            exit_time = entry_time + timedelta(hours=random.randint(2, 6))
            
            visitor = {
                'visitor_id': f"VIS_{random.randint(10000, 99999)}",
                'ticket_type': random.choice(self.ticket_types),
                'entry_time': entry_time,
                'exit_time': exit_time,
                'age_group': random.choice(self.age_groups),
                'group_size': random.randint(1, 8),
                'visited_enclosures': random.sample(self.enclosures, random.randint(2, 6)),
                'total_spent': random.uniform(20, 200),
                'satisfaction_rating': random.randint(1, 5),
                'timestamp': datetime.utcnow()
            }
            visitors.append(visitor)
        
        return visitors
    
    def generate_weather_data(self, location: str = "Zoo Central") -> Dict[str, Any]:
        """Generate mock weather data."""
        conditions = list(WeatherCondition)
        
        return {
            'location': location,
            'temperature': random.uniform(15, 35),
            'humidity': random.uniform(30, 80),
            'pressure': random.uniform(1000, 1020),
            'wind_speed': random.uniform(0, 20),
            'wind_direction': random.uniform(0, 360),
            'condition': random.choice(conditions).value,
            'visibility': random.uniform(5, 20),
            'uv_index': random.uniform(0, 10),
            'timestamp': datetime.utcnow()
        }
    
    def generate_sensor_data(self, count: int = 1) -> List[Dict[str, Any]]:
        """Generate mock sensor data."""
        sensors = []
        sensor_generator = SensorDataGenerator()
        
        for i in range(count):
            sensor_id = f"SENS_{random.randint(1000, 9999)}"
            sensor_type = random.choice(sensor_generator.sensor_types)
            enclosure_id = random.choice(sensor_generator.enclosures)
            
            sensor_data = sensor_generator.generate_sensor_data(sensor_id, sensor_type, enclosure_id)
            sensors.append(sensor_data)
        
        return sensors
    
    def generate_feeding_data(self, count: int = 1) -> List[Dict[str, Any]]:
        """Generate mock feeding data."""
        feedings = []
        food_types = ['meat', 'vegetables', 'fruits', 'fish', 'insects', 'hay', 'pellets']
        
        for i in range(count):
            feeding = {
                'feeding_id': f"FEED_{random.randint(1000, 9999)}",
                'animal_id': f"ANM_{random.randint(1000, 9999)}",
                'food_type': random.choice(food_types),
                'quantity': random.uniform(0.5, 10.0),
                'feeding_time': datetime.utcnow() - timedelta(hours=random.randint(0, 6)),
                'keeper_id': f"KEEPER_{random.randint(100, 999)}",
                'notes': random.choice(['Regular feeding', 'Special diet', 'Medication added', '']),
                'consumed_amount': random.uniform(0.3, 8.0),
                'timestamp': datetime.utcnow()
            }
            feedings.append(feeding)
        
        return feedings


class DataSourceManager:
    """Manages all data sources and provides unified interface."""
    
    def __init__(self):
        self.zoo_api = ZooAPIClient()
        self.weather_api = WeatherAPIClient()
        self.mock_generator = MockDataGenerator()
        self.sensor_generator = SensorDataGenerator()
    
    def get_animal_data(self, use_mock: bool = True, limit: int = 100) -> List[Dict[str, Any]]:
        """Get animal data from API or generate mock data."""
        if use_mock:
            return self.mock_generator.generate_animal_data(limit)
        else:
            return self.zoo_api.get_animals(limit)
    
    def get_visitor_data(self, use_mock: bool = True, limit: int = 100) -> List[Dict[str, Any]]:
        """Get visitor data from API or generate mock data."""
        if use_mock:
            return self.mock_generator.generate_visitor_data(limit)
        else:
            return self.zoo_api.get_visitors()
    
    def get_weather_data(self, location: str = "Zoo Central") -> Dict[str, Any]:
        """Get weather data from API or generate mock data."""
        weather_data = self.weather_api.get_current_weather(location)
        if weather_data:
            return weather_data
        else:
            return self.mock_generator.generate_weather_data(location)
    
    def get_sensor_data(self, count: int = 50) -> List[Dict[str, Any]]:
        """Generate sensor data."""
        return self.mock_generator.generate_sensor_data(count)
    
    def get_feeding_data(self, count: int = 20) -> List[Dict[str, Any]]:
        """Get feeding data from API or generate mock data."""
        if hasattr(self.zoo_api, 'get_feeding_schedule'):
            feeding_data = self.zoo_api.get_feeding_schedule()
            if feeding_data:
                return feeding_data
        
        return self.mock_generator.generate_feeding_data(count)
    
    def stream_data(self, data_type: str, interval: int = 60, 
                   use_mock: bool = True) -> Generator[Dict[str, Any], None, None]:
        """Stream data continuously with specified interval."""
        while True:
            try:
                if data_type == 'animals':
                    data = self.get_animal_data(use_mock, 10)
                elif data_type == 'visitors':
                    data = self.get_visitor_data(use_mock, 10)
                elif data_type == 'weather':
                    data = [self.get_weather_data()]
                elif data_type == 'sensors':
                    data = self.get_sensor_data(20)
                elif data_type == 'feeding':
                    data = self.get_feeding_data(5)
                else:
                    logger.error(f"Unknown data type: {data_type}")
                    break
                
                for item in data:
                    yield item
                
                time.sleep(interval)
                
            except KeyboardInterrupt:
                logger.info("Data streaming interrupted")
                break
            except Exception as e:
                logger.error(f"Error in data streaming: {e}")
                time.sleep(interval)


# Global data source manager instance
data_source_manager = DataSourceManager() 