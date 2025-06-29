-- Zoo Digital Platform - Silver Layer Schema
-- This schema defines the cleaned and validated data tables for the silver layer

-- =====================================================
-- ANIMAL DATA TABLES
-- =====================================================

-- Silver layer animal data table
CREATE TABLE IF NOT EXISTS silver_animals (
    id BIGSERIAL PRIMARY KEY,
    animal_id VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    species VARCHAR(50) NOT NULL,
    age INTEGER NOT NULL CHECK (age >= 0),
    weight_kg DECIMAL(8,2) NOT NULL CHECK (weight_kg > 0),
    status VARCHAR(30) NOT NULL,
    enclosure_id VARCHAR(20) NOT NULL,
    last_feeding TIMESTAMP,
    temperature_celsius DECIMAL(4,1) CHECK (temperature_celsius BETWEEN 20 AND 50),
    heart_rate_bpm INTEGER CHECK (heart_rate_bpm BETWEEN 20 AND 300),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    source_file VARCHAR(255),
    ingestion_batch_id VARCHAR(100)
);

-- Animal health history for tracking changes
CREATE TABLE IF NOT EXISTS silver_animal_health_history (
    id BIGSERIAL PRIMARY KEY,
    animal_id VARCHAR(50) NOT NULL,
    status VARCHAR(30) NOT NULL,
    temperature_celsius DECIMAL(4,1),
    heart_rate_bpm INTEGER,
    recorded_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (animal_id) REFERENCES silver_animals(animal_id)
);

-- =====================================================
-- VISITOR DATA TABLES
-- =====================================================

-- Silver layer visitor data table
CREATE TABLE IF NOT EXISTS silver_visitors (
    id BIGSERIAL PRIMARY KEY,
    visitor_id VARCHAR(50) NOT NULL UNIQUE,
    ticket_type VARCHAR(30) NOT NULL,
    entry_time TIMESTAMP NOT NULL,
    exit_time TIMESTAMP,
    age_group VARCHAR(20) NOT NULL,
    group_size INTEGER NOT NULL CHECK (group_size > 0),
    total_spent_usd DECIMAL(10,2) NOT NULL CHECK (total_spent_usd >= 0),
    satisfaction_rating INTEGER CHECK (satisfaction_rating BETWEEN 1 AND 5),
    visit_duration_minutes INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    source_file VARCHAR(255),
    ingestion_batch_id VARCHAR(100)
);

-- Visitor enclosure visits (normalized)
CREATE TABLE IF NOT EXISTS silver_visitor_enclosures (
    id BIGSERIAL PRIMARY KEY,
    visitor_id VARCHAR(50) NOT NULL,
    enclosure_id VARCHAR(20) NOT NULL,
    visit_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (visitor_id) REFERENCES silver_visitors(visitor_id),
    UNIQUE(visitor_id, enclosure_id, visit_date)
);

-- =====================================================
-- WEATHER DATA TABLES
-- =====================================================

-- Silver layer weather data table
CREATE TABLE IF NOT EXISTS silver_weather (
    id BIGSERIAL PRIMARY KEY,
    location VARCHAR(100) NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    temperature_celsius DECIMAL(4,1) NOT NULL,
    humidity_percent DECIMAL(5,2) NOT NULL CHECK (humidity_percent BETWEEN 0 AND 100),
    pressure_hpa DECIMAL(6,2) NOT NULL CHECK (pressure_hpa > 0),
    wind_speed_ms DECIMAL(4,1) NOT NULL CHECK (wind_speed_ms >= 0),
    wind_direction_degrees DECIMAL(5,1) CHECK (wind_direction_degrees BETWEEN 0 AND 360),
    condition VARCHAR(20) NOT NULL,
    visibility_km DECIMAL(4,1) NOT NULL CHECK (visibility_km >= 0),
    uv_index DECIMAL(3,1) CHECK (uv_index >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    source_file VARCHAR(255),
    ingestion_batch_id VARCHAR(100),
    UNIQUE(location, recorded_at)
);

-- =====================================================
-- SENSOR DATA TABLES
-- =====================================================

-- Silver layer sensor data table
CREATE TABLE IF NOT EXISTS silver_sensors (
    id BIGSERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) NOT NULL,
    sensor_type VARCHAR(30) NOT NULL,
    enclosure_id VARCHAR(20) NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    temperature_celsius DECIMAL(4,1),
    humidity_percent DECIMAL(5,2) CHECK (humidity_percent BETWEEN 0 AND 100),
    air_quality_index DECIMAL(6,1) CHECK (air_quality_index BETWEEN 0 AND 500),
    water_ph DECIMAL(3,1) CHECK (water_ph BETWEEN 0 AND 14),
    water_temperature_celsius DECIMAL(4,1),
    light_intensity_lux DECIMAL(8,1) CHECK (light_intensity_lux >= 0),
    noise_level_db DECIMAL(4,1) CHECK (noise_level_db >= 0),
    battery_level_percent DECIMAL(5,2) CHECK (battery_level_percent BETWEEN 0 AND 100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    source_file VARCHAR(255),
    ingestion_batch_id VARCHAR(100),
    UNIQUE(sensor_id, recorded_at)
);

-- =====================================================
-- FEEDING DATA TABLES
-- =====================================================

-- Silver layer feeding data table
CREATE TABLE IF NOT EXISTS silver_feeding (
    id BIGSERIAL PRIMARY KEY,
    feeding_id VARCHAR(50) NOT NULL UNIQUE,
    animal_id VARCHAR(50) NOT NULL,
    food_type VARCHAR(50) NOT NULL,
    quantity_kg DECIMAL(6,2) NOT NULL CHECK (quantity_kg > 0),
    feeding_time TIMESTAMP NOT NULL,
    keeper_id VARCHAR(20) NOT NULL,
    notes TEXT,
    consumed_amount_kg DECIMAL(6,2) CHECK (consumed_amount_kg >= 0),
    consumption_percentage DECIMAL(5,2) CHECK (consumption_percentage BETWEEN 0 AND 100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    source_file VARCHAR(255),
    ingestion_batch_id VARCHAR(100),
    FOREIGN KEY (animal_id) REFERENCES silver_animals(animal_id)
);

-- =====================================================
-- ENCLOSURE DATA TABLES
-- =====================================================

-- Enclosure information
CREATE TABLE IF NOT EXISTS silver_enclosures (
    id BIGSERIAL PRIMARY KEY,
    enclosure_id VARCHAR(20) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    type VARCHAR(50) NOT NULL,
    capacity INTEGER CHECK (capacity > 0),
    area_sqm DECIMAL(8,2) CHECK (area_sqm > 0),
    temperature_range_min DECIMAL(4,1),
    temperature_range_max DECIMAL(4,1),
    humidity_range_min DECIMAL(5,2),
    humidity_range_max DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- DATA QUALITY AND METADATA TABLES
-- =====================================================

-- Data quality metrics
CREATE TABLE IF NOT EXISTS silver_data_quality (
    id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    batch_id VARCHAR(100) NOT NULL,
    total_records INTEGER NOT NULL,
    valid_records INTEGER NOT NULL,
    invalid_records INTEGER NOT NULL,
    quality_score DECIMAL(3,2) NOT NULL,
    processing_time_seconds DECIMAL(8,2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    errors TEXT,
    warnings TEXT
);

-- Processing metadata
CREATE TABLE IF NOT EXISTS silver_processing_metadata (
    id BIGSERIAL PRIMARY KEY,
    batch_id VARCHAR(100) NOT NULL UNIQUE,
    source_type VARCHAR(50) NOT NULL,
    source_file VARCHAR(255),
    records_processed INTEGER NOT NULL,
    processing_start_time TIMESTAMP NOT NULL,
    processing_end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'processing',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- INDEXES FOR PERFORMANCE
-- =====================================================

-- Animal data indexes
CREATE INDEX IF NOT EXISTS idx_silver_animals_species ON silver_animals(species);
CREATE INDEX IF NOT EXISTS idx_silver_animals_status ON silver_animals(status);
CREATE INDEX IF NOT EXISTS idx_silver_animals_enclosure ON silver_animals(enclosure_id);
CREATE INDEX IF NOT EXISTS idx_silver_animals_created_at ON silver_animals(created_at);

-- Visitor data indexes
CREATE INDEX IF NOT EXISTS idx_silver_visitors_entry_time ON silver_visitors(entry_time);
CREATE INDEX IF NOT EXISTS idx_silver_visitors_ticket_type ON silver_visitors(ticket_type);
CREATE INDEX IF NOT EXISTS idx_silver_visitors_age_group ON silver_visitors(age_group);
CREATE INDEX IF NOT EXISTS idx_silver_visitors_created_at ON silver_visitors(created_at);

-- Weather data indexes
CREATE INDEX IF NOT EXISTS idx_silver_weather_location ON silver_weather(location);
CREATE INDEX IF NOT EXISTS idx_silver_weather_recorded_at ON silver_weather(recorded_at);
CREATE INDEX IF NOT EXISTS idx_silver_weather_condition ON silver_weather(condition);

-- Sensor data indexes
CREATE INDEX IF NOT EXISTS idx_silver_sensors_sensor_id ON silver_sensors(sensor_id);
CREATE INDEX IF NOT EXISTS idx_silver_sensors_enclosure ON silver_sensors(enclosure_id);
CREATE INDEX IF NOT EXISTS idx_silver_sensors_recorded_at ON silver_sensors(recorded_at);
CREATE INDEX IF NOT EXISTS idx_silver_sensors_type ON silver_sensors(sensor_type);

-- Feeding data indexes
CREATE INDEX IF NOT EXISTS idx_silver_feeding_animal_id ON silver_feeding(animal_id);
CREATE INDEX IF NOT EXISTS idx_silver_feeding_feeding_time ON silver_feeding(feeding_time);
CREATE INDEX IF NOT EXISTS idx_silver_feeding_keeper_id ON silver_feeding(keeper_id);

-- Data quality indexes
CREATE INDEX IF NOT EXISTS idx_silver_data_quality_batch ON silver_data_quality(batch_id);
CREATE INDEX IF NOT EXISTS idx_silver_data_quality_table ON silver_data_quality(table_name);
CREATE INDEX IF NOT EXISTS idx_silver_data_quality_processed_at ON silver_data_quality(processed_at);

-- =====================================================
-- VIEWS FOR COMMON QUERIES
-- =====================================================

-- Animal health summary view
CREATE OR REPLACE VIEW silver_animal_health_summary AS
SELECT 
    a.animal_id,
    a.name,
    a.species,
    a.status,
    a.temperature_celsius,
    a.heart_rate_bpm,
    a.last_feeding,
    a.enclosure_id,
    a.data_quality_score,
    a.updated_at
FROM silver_animals a
WHERE a.data_quality_score >= 0.8;

-- Visitor analytics view
CREATE OR REPLACE VIEW silver_visitor_analytics AS
SELECT 
    DATE(entry_time) as visit_date,
    COUNT(*) as total_visitors,
    AVG(total_spent_usd) as avg_spending,
    AVG(satisfaction_rating) as avg_satisfaction,
    SUM(total_spent_usd) as total_revenue
FROM silver_visitors
WHERE data_quality_score >= 0.8
GROUP BY DATE(entry_time);

-- Enclosure sensor summary view
CREATE OR REPLACE VIEW silver_enclosure_sensor_summary AS
SELECT 
    enclosure_id,
    sensor_type,
    AVG(temperature_celsius) as avg_temperature,
    AVG(humidity_percent) as avg_humidity,
    AVG(air_quality_index) as avg_air_quality,
    COUNT(*) as reading_count,
    MAX(recorded_at) as last_reading
FROM silver_sensors
WHERE data_quality_score >= 0.8
GROUP BY enclosure_id, sensor_type;

-- =====================================================
-- TRIGGERS FOR AUTOMATIC UPDATES
-- =====================================================

-- Update updated_at timestamp trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply triggers to tables with updated_at columns
CREATE TRIGGER update_silver_animals_updated_at 
    BEFORE UPDATE ON silver_animals 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_silver_visitors_updated_at 
    BEFORE UPDATE ON silver_visitors 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_silver_enclosures_updated_at 
    BEFORE UPDATE ON silver_enclosures 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_silver_processing_metadata_updated_at 
    BEFORE UPDATE ON silver_processing_metadata 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column(); 