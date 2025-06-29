import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any
from data.ingestion.models import (
    AnimalSpecies, AnimalStatus, WeatherCondition,
    AnimalData, VisitorData, WeatherData, SensorData, FeedingData
)


class DataGenerator:
    """Generates realistic zoo data for testing and development."""
    
    def __init__(self):
        self.animal_names = {
            AnimalSpecies.LION: ["Simba", "Nala", "Mufasa", "Sarabi", "Scar", "Kiara", "Kovu"],
            AnimalSpecies.TIGER: ["Rajah", "Shere Khan", "Tigger", "Tiger Lily", "Shadow", "Stripe"],
            AnimalSpecies.ELEPHANT: ["Dumbo", "Jumbo", "Ellie", "Tantor", "Big Ears", "Trunk"],
            AnimalSpecies.GIRAFFE: ["Melman", "Gerald", "Tallulah", "Necky", "Spot", "Long Legs"],
            AnimalSpecies.PENGUIN: ["Happy Feet", "Mumble", "Gloria", "Rico", "Skipper", "Kowalski"],
            AnimalSpecies.GORILLA: ["King Kong", "Harambe", "Koko", "Binti", "Jambo", "Silverback"],
            AnimalSpecies.ZEBRA: ["Marty", "Stripes", "Zigzag", "Zebra", "Black", "White"],
            AnimalSpecies.RHINO: ["Rhinoceros", "Horn", "Tank", "Armor", "Thick Skin"],
            AnimalSpecies.PANDA: ["Po", "Panda", "Bamboo", "Black Eye", "White Fur"],
            AnimalSpecies.KANGAROO: ["Roo", "Kanga", "Joey", "Bouncy", "Pouch", "Hop"]
        }
        
        self.enclosure_ids = ["E001", "E002", "E003", "E004", "E005", "E006", "E007", "E008", "E009", "E010"]
        self.keeper_ids = ["K001", "K002", "K003", "K004", "K005"]
        self.sensor_ids = ["S001", "S002", "S003", "S004", "S005", "S006", "S007", "S008"]
        
    def generate_animal_data(self, count: int = 1) -> List[Dict[str, Any]]:
        """Generate animal data records."""
        animals = []
        
        for _ in range(count):
            species = random.choice(list(AnimalSpecies))
            name = random.choice(self.animal_names[species])
            
            animal = {
                "animal_id": f"A{str(uuid.uuid4())[:8].upper()}",
                "name": name,
                "species": species.value,
                "age": random.randint(1, 25),
                "weight": round(random.uniform(50, 5000), 2),
                "status": random.choice(list(AnimalStatus)).value,
                "enclosure_id": random.choice(self.enclosure_ids),
                "last_feeding": datetime.utcnow() - timedelta(hours=random.randint(1, 12)),
                "temperature": round(random.uniform(35, 42), 1),
                "heart_rate": random.randint(60, 120),
                "timestamp": datetime.utcnow()
            }
            animals.append(animal)
        
        return animals
    
    def generate_visitor_data(self, count: int = 1) -> List[Dict[str, Any]]:
        """Generate visitor data records."""
        visitors = []
        
        ticket_types = ["adult", "child", "senior", "family", "group"]
        age_groups = ["child", "teen", "adult", "senior"]
        
        for _ in range(count):
            entry_time = datetime.utcnow() - timedelta(hours=random.randint(1, 8))
            exit_time = entry_time + timedelta(hours=random.randint(1, 4)) if random.random() > 0.3 else None
            
            visitor = {
                "visitor_id": f"V{str(uuid.uuid4())[:8].upper()}",
                "ticket_type": random.choice(ticket_types),
                "entry_time": entry_time,
                "exit_time": exit_time,
                "age_group": random.choice(age_groups),
                "group_size": random.randint(1, 8),
                "visited_enclosures": random.sample(self.enclosure_ids, random.randint(1, 5)),
                "total_spent": round(random.uniform(10, 200), 2),
                "satisfaction_rating": random.randint(1, 5) if random.random() > 0.2 else None,
                "timestamp": datetime.utcnow()
            }
            visitors.append(visitor)
        
        return visitors
    
    def generate_weather_data(self, count: int = 1) -> List[Dict[str, Any]]:
        """Generate weather data records."""
        weather_records = []
        
        for _ in range(count):
            weather = {
                "location": "Zoo Central",
                "temperature": round(random.uniform(-10, 35), 1),
                "humidity": round(random.uniform(30, 90), 1),
                "pressure": round(random.uniform(980, 1030), 1),
                "wind_speed": round(random.uniform(0, 25), 1),
                "wind_direction": random.randint(0, 360),
                "condition": random.choice(list(WeatherCondition)).value,
                "visibility": round(random.uniform(0, 20), 1),
                "uv_index": round(random.uniform(0, 11), 1) if random.random() > 0.3 else None,
                "timestamp": datetime.utcnow()
            }
            weather_records.append(weather)
        
        return weather_records
    
    def generate_sensor_data(self, count: int = 1) -> List[Dict[str, Any]]:
        """Generate sensor data records."""
        sensor_records = []
        
        sensor_types = ["temperature", "humidity", "air_quality", "water_ph", "light", "noise"]
        
        for _ in range(count):
            sensor_type = random.choice(sensor_types)
            sensor_data = {
                "sensor_id": random.choice(self.sensor_ids),
                "sensor_type": sensor_type,
                "enclosure_id": random.choice(self.enclosure_ids),
                "timestamp": datetime.utcnow()
            }
            
            # Add sensor-specific readings
            if sensor_type == "temperature":
                sensor_data["temperature"] = round(random.uniform(15, 35), 1)
            elif sensor_type == "humidity":
                sensor_data["humidity"] = round(random.uniform(30, 90), 1)
            elif sensor_type == "air_quality":
                sensor_data["air_quality"] = round(random.uniform(0, 200), 1)
            elif sensor_type == "water_ph":
                sensor_data["water_ph"] = round(random.uniform(6.5, 8.5), 2)
                sensor_data["water_temperature"] = round(random.uniform(15, 25), 1)
            elif sensor_type == "light":
                sensor_data["light_intensity"] = round(random.uniform(0, 1000), 1)
            elif sensor_type == "noise":
                sensor_data["noise_level"] = round(random.uniform(30, 80), 1)
            
            # Add common fields
            sensor_data["battery_level"] = round(random.uniform(20, 100), 1)
            
            sensor_records.append(sensor_data)
        
        return sensor_records
    
    def generate_feeding_data(self, count: int = 1) -> List[Dict[str, Any]]:
        """Generate feeding data records."""
        feeding_records = []
        
        food_types = {
            AnimalSpecies.LION: ["meat", "chicken", "beef"],
            AnimalSpecies.TIGER: ["meat", "pork", "beef"],
            AnimalSpecies.ELEPHANT: ["hay", "vegetables", "fruits"],
            AnimalSpecies.GIRAFFE: ["leaves", "hay", "vegetables"],
            AnimalSpecies.PENGUIN: ["fish", "squid", "krill"],
            AnimalSpecies.GORILLA: ["fruits", "vegetables", "nuts"],
            AnimalSpecies.ZEBRA: ["grass", "hay", "vegetables"],
            AnimalSpecies.RHINO: ["grass", "hay", "vegetables"],
            AnimalSpecies.PANDA: ["bamboo", "vegetables", "fruits"],
            AnimalSpecies.KANGAROO: ["grass", "vegetables", "fruits"]
        }
        
        for _ in range(count):
            species = random.choice(list(AnimalSpecies))
            food_type = random.choice(food_types[species])
            
            feeding = {
                "feeding_id": f"F{str(uuid.uuid4())[:8].upper()}",
                "animal_id": f"A{str(uuid.uuid4())[:8].upper()}",
                "food_type": food_type,
                "quantity": round(random.uniform(0.5, 10), 2),
                "feeding_time": datetime.utcnow() - timedelta(hours=random.randint(0, 6)),
                "keeper_id": random.choice(self.keeper_ids),
                "notes": random.choice([None, "Regular feeding", "Special diet", "Extra portion"]),
                "consumed_amount": round(random.uniform(0.3, 8), 2) if random.random() > 0.2 else None,
                "timestamp": datetime.utcnow()
            }
            feeding_records.append(feeding)
        
        return feeding_records
    
    def generate_all_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """Generate all types of data at once."""
        return {
            "animals": self.generate_animal_data(5),
            "visitors": self.generate_visitor_data(10),
            "weather": self.generate_weather_data(1),
            "sensors": self.generate_sensor_data(8),
            "feeding": self.generate_feeding_data(4)
        } 