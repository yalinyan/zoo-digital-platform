-- Zoo Digital Platform - Silver Layer Validation Job
-- This script performs comprehensive data validation and quality checks
-- Run this independently from cleaning jobs to assess data quality

-- =============================
-- 1. ANIMAL DATA VALIDATION
-- =============================

-- Check for animals with invalid age ranges
INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at, errors)
SELECT 
    'silver_animals_validation',
    CONCAT('validation_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*),
    COUNT(*) FILTER (WHERE age >= 0 AND age <= 100),
    COUNT(*) FILTER (WHERE age < 0 OR age > 100),
    AVG(data_quality_score),
    NOW(),
    'Animals with invalid age: ' || COUNT(*) FILTER (WHERE age < 0 OR age > 100)
FROM silver_animals;

-- Check for animals with extreme weight values
INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at, warnings)
SELECT 
    'silver_animals_weight_check',
    CONCAT('validation_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*),
    COUNT(*) FILTER (WHERE weight_kg BETWEEN 0.1 AND 10000),
    COUNT(*) FILTER (WHERE weight_kg < 0.1 OR weight_kg > 10000),
    AVG(data_quality_score),
    NOW(),
    'Animals with extreme weight values: ' || COUNT(*) FILTER (WHERE weight_kg < 0.1 OR weight_kg > 10000)
FROM silver_animals;

-- Check for animals with health anomalies
INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at, warnings)
SELECT 
    'silver_animals_health_check',
    CONCAT('validation_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*),
    COUNT(*) FILTER (WHERE 
        (temperature_celsius IS NULL OR (temperature_celsius BETWEEN 20 AND 50)) AND
        (heart_rate_bpm IS NULL OR (heart_rate_bpm BETWEEN 20 AND 300))
    ),
    COUNT(*) FILTER (WHERE 
        (temperature_celsius IS NOT NULL AND (temperature_celsius < 20 OR temperature_celsius > 50)) OR
        (heart_rate_bpm IS NOT NULL AND (heart_rate_bpm < 20 OR heart_rate_bpm > 300))
    ),
    AVG(data_quality_score),
    NOW(),
    'Animals with health anomalies: ' || COUNT(*) FILTER (WHERE 
        (temperature_celsius IS NOT NULL AND (temperature_celsius < 20 OR temperature_celsius > 50)) OR
        (heart_rate_bpm IS NOT NULL AND (heart_rate_bpm < 20 OR heart_rate_bpm > 300))
    )
FROM silver_animals;

-- =============================
-- 2. VISITOR DATA VALIDATION
-- =============================

-- Check for visitors with invalid spending patterns
INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at, warnings)
SELECT 
    'silver_visitors_spending_check',
    CONCAT('validation_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*),
    COUNT(*) FILTER (WHERE total_spent_usd BETWEEN 0 AND 10000),
    COUNT(*) FILTER (WHERE total_spent_usd < 0 OR total_spent_usd > 10000),
    AVG(data_quality_score),
    NOW(),
    'Visitors with invalid spending: ' || COUNT(*) FILTER (WHERE total_spent_usd < 0 OR total_spent_usd > 10000)
FROM silver_visitors;

-- Check for visitors with impossible visit durations
INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at, warnings)
SELECT 
    'silver_visitors_duration_check',
    CONCAT('validation_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*),
    COUNT(*) FILTER (WHERE 
        visit_duration_minutes IS NULL OR 
        (visit_duration_minutes >= 0 AND visit_duration_minutes <= 1440)
    ),
    COUNT(*) FILTER (WHERE 
        visit_duration_minutes IS NOT NULL AND 
        (visit_duration_minutes < 0 OR visit_duration_minutes > 1440)
    ),
    AVG(data_quality_score),
    NOW(),
    'Visitors with impossible duration: ' || COUNT(*) FILTER (WHERE 
        visit_duration_minutes IS NOT NULL AND 
        (visit_duration_minutes < 0 OR visit_duration_minutes > 1440)
    )
FROM silver_visitors;

-- Check for satisfaction rating anomalies
INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at, warnings)
SELECT 
    'silver_visitors_satisfaction_check',
    CONCAT('validation_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*),
    COUNT(*) FILTER (WHERE 
        satisfaction_rating IS NULL OR 
        (satisfaction_rating BETWEEN 1 AND 5)
    ),
    COUNT(*) FILTER (WHERE 
        satisfaction_rating IS NOT NULL AND 
        (satisfaction_rating < 1 OR satisfaction_rating > 5)
    ),
    AVG(data_quality_score),
    NOW(),
    'Visitors with invalid satisfaction rating: ' || COUNT(*) FILTER (WHERE 
        satisfaction_rating IS NOT NULL AND 
        (satisfaction_rating < 1 OR satisfaction_rating > 5)
    )
FROM silver_visitors;

-- =============================
-- 3. WEATHER DATA VALIDATION
-- =============================

-- Check for weather data anomalies
INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at, warnings)
SELECT 
    'silver_weather_anomaly_check',
    CONCAT('validation_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*),
    COUNT(*) FILTER (WHERE 
        temperature_celsius BETWEEN -50 AND 60 AND
        humidity_percent BETWEEN 0 AND 100 AND
        pressure_hpa BETWEEN 800 AND 1200 AND
        wind_speed_ms BETWEEN 0 AND 200
    ),
    COUNT(*) FILTER (WHERE 
        temperature_celsius < -50 OR temperature_celsius > 60 OR
        humidity_percent < 0 OR humidity_percent > 100 OR
        pressure_hpa < 800 OR pressure_hpa > 1200 OR
        wind_speed_ms < 0 OR wind_speed_ms > 200
    ),
    AVG(data_quality_score),
    NOW(),
    'Weather records with anomalies: ' || COUNT(*) FILTER (WHERE 
        temperature_celsius < -50 OR temperature_celsius > 60 OR
        humidity_percent < 0 OR humidity_percent > 100 OR
        pressure_hpa < 800 OR pressure_hpa > 1200 OR
        wind_speed_ms < 0 OR wind_speed_ms > 200
    )
FROM silver_weather;

-- Check for missing weather data
INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at, warnings)
SELECT 
    'silver_weather_completeness_check',
    CONCAT('validation_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*),
    COUNT(*) FILTER (WHERE 
        temperature_celsius IS NOT NULL AND
        humidity_percent IS NOT NULL AND
        pressure_hpa IS NOT NULL AND
        wind_speed_ms IS NOT NULL AND
        condition IS NOT NULL
    ),
    COUNT(*) FILTER (WHERE 
        temperature_celsius IS NULL OR
        humidity_percent IS NULL OR
        pressure_hpa IS NULL OR
        wind_speed_ms IS NULL OR
        condition IS NULL
    ),
    AVG(data_quality_score),
    NOW(),
    'Weather records with missing data: ' || COUNT(*) FILTER (WHERE 
        temperature_celsius IS NULL OR
        humidity_percent IS NULL OR
        pressure_hpa IS NULL OR
        wind_speed_ms IS NULL OR
        condition IS NULL
    )
FROM silver_weather;

-- =============================
-- 4. SENSOR DATA VALIDATION
-- =============================

-- Check for sensor data completeness by type
INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at, warnings)
SELECT 
    'silver_sensors_completeness_check',
    CONCAT('validation_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*),
    COUNT(*) FILTER (WHERE 
        CASE 
            WHEN sensor_type = 'temperature' THEN temperature_celsius IS NOT NULL
            WHEN sensor_type = 'humidity' THEN humidity_percent IS NOT NULL
            WHEN sensor_type = 'air_quality' THEN air_quality_index IS NOT NULL
            WHEN sensor_type = 'water_ph' THEN water_ph IS NOT NULL AND water_temperature_celsius IS NOT NULL
            WHEN sensor_type = 'light' THEN light_intensity_lux IS NOT NULL
            WHEN sensor_type = 'noise' THEN noise_level_db IS NOT NULL
            ELSE TRUE
        END
    ),
    COUNT(*) FILTER (WHERE 
        CASE 
            WHEN sensor_type = 'temperature' THEN temperature_celsius IS NULL
            WHEN sensor_type = 'humidity' THEN humidity_percent IS NULL
            WHEN sensor_type = 'air_quality' THEN air_quality_index IS NULL
            WHEN sensor_type = 'water_ph' THEN water_ph IS NULL OR water_temperature_celsius IS NULL
            WHEN sensor_type = 'light' THEN light_intensity_lux IS NULL
            WHEN sensor_type = 'noise' THEN noise_level_db IS NULL
            ELSE FALSE
        END
    ),
    AVG(data_quality_score),
    NOW(),
    'Sensor records with missing required data: ' || COUNT(*) FILTER (WHERE 
        CASE 
            WHEN sensor_type = 'temperature' THEN temperature_celsius IS NULL
            WHEN sensor_type = 'humidity' THEN humidity_percent IS NULL
            WHEN sensor_type = 'air_quality' THEN air_quality_index IS NULL
            WHEN sensor_type = 'water_ph' THEN water_ph IS NULL OR water_temperature_celsius IS NULL
            WHEN sensor_type = 'light' THEN light_intensity_lux IS NULL
            WHEN sensor_type = 'noise' THEN noise_level_db IS NULL
            ELSE FALSE
        END
    )
FROM silver_sensors;

-- Check for sensor battery levels
INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at, warnings)
SELECT 
    'silver_sensors_battery_check',
    CONCAT('validation_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*),
    COUNT(*) FILTER (WHERE 
        battery_level_percent IS NULL OR 
        battery_level_percent BETWEEN 0 AND 100
    ),
    COUNT(*) FILTER (WHERE 
        battery_level_percent IS NOT NULL AND 
        (battery_level_percent < 0 OR battery_level_percent > 100)
    ),
    AVG(data_quality_score),
    NOW(),
    'Sensors with invalid battery levels: ' || COUNT(*) FILTER (WHERE 
        battery_level_percent IS NOT NULL AND 
        (battery_level_percent < 0 OR battery_level_percent > 100)
    )
FROM silver_sensors;

-- =============================
-- 5. FEEDING DATA VALIDATION
-- =============================

-- Check for feeding data anomalies
INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at, warnings)
SELECT 
    'silver_feeding_anomaly_check',
    CONCAT('validation_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*),
    COUNT(*) FILTER (WHERE 
        quantity_kg > 0 AND
        (consumed_amount_kg IS NULL OR consumed_amount_kg >= 0) AND
        (consumption_percentage IS NULL OR (consumption_percentage >= 0 AND consumption_percentage <= 100))
    ),
    COUNT(*) FILTER (WHERE 
        quantity_kg <= 0 OR
        (consumed_amount_kg IS NOT NULL AND consumed_amount_kg < 0) OR
        (consumption_percentage IS NOT NULL AND (consumption_percentage < 0 OR consumption_percentage > 100))
    ),
    AVG(data_quality_score),
    NOW(),
    'Feeding records with anomalies: ' || COUNT(*) FILTER (WHERE 
        quantity_kg <= 0 OR
        (consumed_amount_kg IS NOT NULL AND consumed_amount_kg < 0) OR
        (consumption_percentage IS NOT NULL AND (consumption_percentage < 0 OR consumption_percentage > 100))
    )
FROM silver_feeding;

-- Check for feeding time consistency
INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at, warnings)
SELECT 
    'silver_feeding_time_check',
    CONCAT('validation_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*),
    COUNT(*) FILTER (WHERE 
        feeding_time IS NOT NULL AND
        feeding_time <= NOW() AND
        feeding_time >= NOW() - INTERVAL '30 days'
    ),
    COUNT(*) FILTER (WHERE 
        feeding_time IS NULL OR
        feeding_time > NOW() OR
        feeding_time < NOW() - INTERVAL '30 days'
    ),
    AVG(data_quality_score),
    NOW(),
    'Feeding records with time issues: ' || COUNT(*) FILTER (WHERE 
        feeding_time IS NULL OR
        feeding_time > NOW() OR
        feeding_time < NOW() - INTERVAL '30 days'
    )
FROM silver_feeding;

-- =============================
-- 6. CROSS-TABLE VALIDATION
-- =============================

-- Check for orphaned feeding records (no corresponding animal)
INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at, errors)
SELECT 
    'silver_feeding_orphan_check',
    CONCAT('validation_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*),
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM silver_animals a WHERE a.animal_id = f.animal_id
    )),
    COUNT(*) FILTER (WHERE NOT EXISTS (
        SELECT 1 FROM silver_animals a WHERE a.animal_id = f.animal_id
    )),
    AVG(data_quality_score),
    NOW(),
    'Feeding records with orphaned animals: ' || COUNT(*) FILTER (WHERE NOT EXISTS (
        SELECT 1 FROM silver_animals a WHERE a.animal_id = f.animal_id
    ))
FROM silver_feeding f;

-- Check for animals in non-existent enclosures
INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at, errors)
SELECT 
    'silver_animals_enclosure_check',
    CONCAT('validation_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*),
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM silver_enclosures e WHERE e.enclosure_id = a.enclosure_id
    )),
    COUNT(*) FILTER (WHERE NOT EXISTS (
        SELECT 1 FROM silver_enclosures e WHERE e.enclosure_id = a.enclosure_id
    )),
    AVG(data_quality_score),
    NOW(),
    'Animals in non-existent enclosures: ' || COUNT(*) FILTER (WHERE NOT EXISTS (
        SELECT 1 FROM silver_enclosures e WHERE e.enclosure_id = a.enclosure_id
    ))
FROM silver_animals a;

-- =============================
-- 7. DATA FRESHNESS VALIDATION
-- =============================

-- Check for stale data (older than 7 days)
INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at, warnings)
SELECT 
    'silver_data_freshness_check',
    CONCAT('validation_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*),
    COUNT(*) FILTER (WHERE updated_at >= NOW() - INTERVAL '7 days'),
    COUNT(*) FILTER (WHERE updated_at < NOW() - INTERVAL '7 days'),
    AVG(data_quality_score),
    NOW(),
    'Records older than 7 days: ' || COUNT(*) FILTER (WHERE updated_at < NOW() - INTERVAL '7 days')
FROM (
    SELECT updated_at, data_quality_score FROM silver_animals
    UNION ALL
    SELECT updated_at, data_quality_score FROM silver_visitors
    UNION ALL
    SELECT created_at as updated_at, data_quality_score FROM silver_weather
    UNION ALL
    SELECT created_at as updated_at, data_quality_score FROM silver_sensors
    UNION ALL
    SELECT created_at as updated_at, data_quality_score FROM silver_feeding
) all_records;

-- =============================
-- 8. SUMMARY VALIDATION REPORT
-- =============================

-- Create a summary view of all validation results
CREATE OR REPLACE VIEW silver_validation_summary AS
SELECT 
    table_name,
    batch_id,
    total_records,
    valid_records,
    invalid_records,
    quality_score,
    processed_at,
    errors,
    warnings
FROM silver_data_quality 
WHERE table_name LIKE '%validation%' OR table_name LIKE '%check%'
ORDER BY processed_at DESC;

-- =============================
-- END OF VALIDATION JOB 