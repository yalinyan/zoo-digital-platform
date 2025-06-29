from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class AnimalSpecies(str, Enum):
    LION = "lion"
    TIGER = "tiger"
    ELEPHANT = "elephant"
    GIRAFFE = "giraffe"
    PENGUIN = "penguin"
    GORILLA = "gorilla"
    ZEBRA = "zebra"
    RHINO = "rhino"
    PANDA = "panda"
    KANGAROO = "kangaroo"


class AnimalStatus(str, Enum):
    HEALTHY = "healthy"
    SICK = "sick"
    INJURED = "injured"
    UNDER_TREATMENT = "under_treatment"
    QUARANTINE = "quarantine"


class WeatherCondition(str, Enum):
    SUNNY = "sunny"
    CLOUDY = "cloudy"
    RAINY = "rainy"
    SNOWY = "snowy"
    WINDY = "windy"
    STORMY = "stormy"


class AnimalData(BaseModel):
    animal_id: str = Field(..., description="Unique identifier for the animal")
    name: str = Field(..., description="Animal's name")
    species: AnimalSpecies = Field(..., description="Animal species")
    age: int = Field(..., ge=0, description="Animal's age in years")
    weight: float = Field(..., gt=0, description="Animal's weight in kg")
    status: AnimalStatus = Field(..., description="Current health status")
    enclosure_id: str = Field(..., description="Enclosure where animal is located")
    last_feeding: Optional[datetime] = Field(None, description="Last feeding time")
    temperature: Optional[float] = Field(None, description="Body temperature in Celsius")
    heart_rate: Optional[int] = Field(None, ge=0, description="Heart rate in BPM")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Data timestamp")
    
    @validator('temperature')
    def validate_temperature(cls, v):
        if v is not None and (v < 20 or v > 50):
            raise ValueError('Temperature must be between 20 and 50 Celsius')
        return v
    
    @validator('heart_rate')
    def validate_heart_rate(cls, v):
        if v is not None and (v < 20 or v > 300):
            raise ValueError('Heart rate must be between 20 and 300 BPM')
        return v


class VisitorData(BaseModel):
    visitor_id: str = Field(..., description="Unique visitor identifier")
    ticket_type: str = Field(..., description="Type of ticket purchased")
    entry_time: datetime = Field(..., description="Entry timestamp")
    exit_time: Optional[datetime] = Field(None, description="Exit timestamp")
    age_group: str = Field(..., description="Age group category")
    group_size: int = Field(..., ge=1, description="Number of people in group")
    visited_enclosures: List[str] = Field(default_factory=list, description="List of visited enclosures")
    total_spent: float = Field(..., ge=0, description="Total amount spent in USD")
    satisfaction_rating: Optional[int] = Field(None, ge=1, le=5, description="Satisfaction rating 1-5")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Data timestamp")


class WeatherData(BaseModel):
    location: str = Field(..., description="Zoo location")
    temperature: float = Field(..., description="Temperature in Celsius")
    humidity: float = Field(..., ge=0, le=100, description="Humidity percentage")
    pressure: float = Field(..., gt=0, description="Atmospheric pressure in hPa")
    wind_speed: float = Field(..., ge=0, description="Wind speed in m/s")
    wind_direction: float = Field(..., ge=0, le=360, description="Wind direction in degrees")
    condition: WeatherCondition = Field(..., description="Weather condition")
    visibility: float = Field(..., ge=0, description="Visibility in km")
    uv_index: Optional[float] = Field(None, ge=0, description="UV index")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Data timestamp")


class SensorData(BaseModel):
    sensor_id: str = Field(..., description="Unique sensor identifier")
    sensor_type: str = Field(..., description="Type of sensor")
    enclosure_id: str = Field(..., description="Enclosure where sensor is located")
    temperature: Optional[float] = Field(None, description="Temperature reading")
    humidity: Optional[float] = Field(None, ge=0, le=100, description="Humidity reading")
    air_quality: Optional[float] = Field(None, ge=0, le=500, description="Air quality index")
    water_ph: Optional[float] = Field(None, ge=0, le=14, description="Water pH level")
    water_temperature: Optional[float] = Field(None, description="Water temperature")
    light_intensity: Optional[float] = Field(None, ge=0, description="Light intensity in lux")
    noise_level: Optional[float] = Field(None, ge=0, description="Noise level in dB")
    battery_level: Optional[float] = Field(None, ge=0, le=100, description="Battery level percentage")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Data timestamp")


class FeedingData(BaseModel):
    feeding_id: str = Field(..., description="Unique feeding identifier")
    animal_id: str = Field(..., description="Animal being fed")
    food_type: str = Field(..., description="Type of food provided")
    quantity: float = Field(..., gt=0, description="Quantity in kg")
    feeding_time: datetime = Field(..., description="Feeding timestamp")
    keeper_id: str = Field(..., description="Zoo keeper identifier")
    notes: Optional[str] = Field(None, description="Additional notes")
    consumed_amount: Optional[float] = Field(None, ge=0, description="Amount consumed in kg")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Data timestamp")


class IngestionEvent(BaseModel):
    event_id: str = Field(..., description="Unique event identifier")
    source: str = Field(..., description="Data source")
    data_type: str = Field(..., description="Type of data")
    data: Dict[str, Any] = Field(..., description="Actual data payload")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Event timestamp")
    version: str = Field(default="1.0", description="Data schema version") 