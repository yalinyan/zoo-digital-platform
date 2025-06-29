-- Zoo Digital Platform - Silver Layer Cleaning/Validation Job
-- This script is intended to be run on a schedule (e.g., hourly/daily) via Airflow or similar orchestrator.
-- It performs cleaning, deduplication, validation, and data quality metric updates for all silver layer tables.

-- =============================
-- 1. ANIMALS TABLE
-- =============================

-- Create quarantine table if not exists
CREATE TABLE IF NOT EXISTS silver_animals_quarantine AS TABLE silver_animals WITH NO DATA;

-- Remove duplicates (keep latest by updated_at)
DELETE FROM silver_animals a
USING (
    SELECT animal_id, MAX(updated_at) AS max_updated
    FROM silver_animals
    GROUP BY animal_id
    HAVING COUNT(*) > 1
) dups
WHERE a.animal_id = dups.animal_id
  AND a.updated_at < dups.max_updated;

-- Move invalid/low-quality records to quarantine
INSERT INTO silver_animals_quarantine
SELECT * FROM silver_animals WHERE data_quality_score < 0.7
ON CONFLICT DO NOTHING;

DELETE FROM silver_animals WHERE data_quality_score < 0.7;

-- =============================
-- 2. VISITORS TABLE
-- =============================

CREATE TABLE IF NOT EXISTS silver_visitors_quarantine AS TABLE silver_visitors WITH NO DATA;

DELETE FROM silver_visitors v
USING (
    SELECT visitor_id, MAX(updated_at) AS max_updated
    FROM silver_visitors
    GROUP BY visitor_id
    HAVING COUNT(*) > 1
) dups
WHERE v.visitor_id = dups.visitor_id
  AND v.updated_at < dups.max_updated;

INSERT INTO silver_visitors_quarantine
SELECT * FROM silver_visitors WHERE data_quality_score < 0.7
ON CONFLICT DO NOTHING;

DELETE FROM silver_visitors WHERE data_quality_score < 0.7;

-- =============================
-- 3. WEATHER TABLE
-- =============================

CREATE TABLE IF NOT EXISTS silver_weather_quarantine AS TABLE silver_weather WITH NO DATA;

DELETE FROM silver_weather w
USING (
    SELECT location, recorded_at, COUNT(*)
    FROM silver_weather
    GROUP BY location, recorded_at
    HAVING COUNT(*) > 1
) dups
WHERE w.location = dups.location AND w.recorded_at = dups.recorded_at
  AND w.id NOT IN (
    SELECT MIN(id) FROM silver_weather WHERE location = dups.location AND recorded_at = dups.recorded_at
  );

INSERT INTO silver_weather_quarantine
SELECT * FROM silver_weather WHERE data_quality_score < 0.7
ON CONFLICT DO NOTHING;

DELETE FROM silver_weather WHERE data_quality_score < 0.7;

-- =============================
-- 4. SENSORS TABLE
-- =============================

CREATE TABLE IF NOT EXISTS silver_sensors_quarantine AS TABLE silver_sensors WITH NO DATA;

DELETE FROM silver_sensors s
USING (
    SELECT sensor_id, recorded_at, COUNT(*)
    FROM silver_sensors
    GROUP BY sensor_id, recorded_at
    HAVING COUNT(*) > 1
) dups
WHERE s.sensor_id = dups.sensor_id AND s.recorded_at = dups.recorded_at
  AND s.id NOT IN (
    SELECT MIN(id) FROM silver_sensors WHERE sensor_id = dups.sensor_id AND recorded_at = dups.recorded_at
  );

INSERT INTO silver_sensors_quarantine
SELECT * FROM silver_sensors WHERE data_quality_score < 0.7
ON CONFLICT DO NOTHING;

DELETE FROM silver_sensors WHERE data_quality_score < 0.7;

-- =============================
-- 5. FEEDING TABLE
-- =============================

CREATE TABLE IF NOT EXISTS silver_feeding_quarantine AS TABLE silver_feeding WITH NO DATA;

DELETE FROM silver_feeding f
USING (
    SELECT feeding_id, MAX(created_at) AS max_created
    FROM silver_feeding
    GROUP BY feeding_id
    HAVING COUNT(*) > 1
) dups
WHERE f.feeding_id = dups.feeding_id
  AND f.created_at < dups.max_created;

INSERT INTO silver_feeding_quarantine
SELECT * FROM silver_feeding WHERE data_quality_score < 0.7
ON CONFLICT DO NOTHING;

DELETE FROM silver_feeding WHERE data_quality_score < 0.7;

-- =============================
-- 6. ENCLOSURES TABLE
-- =============================

CREATE TABLE IF NOT EXISTS silver_enclosures_quarantine AS TABLE silver_enclosures WITH NO DATA;

DELETE FROM silver_enclosures e
USING (
    SELECT enclosure_id, MAX(updated_at) AS max_updated
    FROM silver_enclosures
    GROUP BY enclosure_id
    HAVING COUNT(*) > 1
) dups
WHERE e.enclosure_id = dups.enclosure_id
  AND e.updated_at < dups.max_updated;

INSERT INTO silver_enclosures_quarantine
SELECT * FROM silver_enclosures WHERE area_sqm IS NULL OR capacity IS NULL OR area_sqm <= 0 OR capacity <= 0
ON CONFLICT DO NOTHING;

DELETE FROM silver_enclosures WHERE area_sqm IS NULL OR capacity IS NULL OR area_sqm <= 0 OR capacity <= 0;

-- =============================
-- 7. DATA QUALITY METRICS
-- =============================

INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at)
SELECT
    'silver_animals',
    CONCAT('cleaning_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*) + (SELECT COUNT(*) FROM silver_animals_quarantine),
    COUNT(*),
    (SELECT COUNT(*) FROM silver_animals_quarantine),
    AVG(data_quality_score),
    NOW()
FROM silver_animals;

INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at)
SELECT
    'silver_visitors',
    CONCAT('cleaning_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*) + (SELECT COUNT(*) FROM silver_visitors_quarantine),
    COUNT(*),
    (SELECT COUNT(*) FROM silver_visitors_quarantine),
    AVG(data_quality_score),
    NOW()
FROM silver_visitors;

INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at)
SELECT
    'silver_weather',
    CONCAT('cleaning_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*) + (SELECT COUNT(*) FROM silver_weather_quarantine),
    COUNT(*),
    (SELECT COUNT(*) FROM silver_weather_quarantine),
    AVG(data_quality_score),
    NOW()
FROM silver_weather;

INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at)
SELECT
    'silver_sensors',
    CONCAT('cleaning_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*) + (SELECT COUNT(*) FROM silver_sensors_quarantine),
    COUNT(*),
    (SELECT COUNT(*) FROM silver_sensors_quarantine),
    AVG(data_quality_score),
    NOW()
FROM silver_sensors;

INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at)
SELECT
    'silver_feeding',
    CONCAT('cleaning_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*) + (SELECT COUNT(*) FROM silver_feeding_quarantine),
    COUNT(*),
    (SELECT COUNT(*) FROM silver_feeding_quarantine),
    AVG(data_quality_score),
    NOW()
FROM silver_feeding;

INSERT INTO silver_data_quality (table_name, batch_id, total_records, valid_records, invalid_records, quality_score, processed_at)
SELECT
    'silver_enclosures',
    CONCAT('cleaning_', TO_CHAR(NOW(), 'YYYYMMDD_HH24MI')),
    COUNT(*) + (SELECT COUNT(*) FROM silver_enclosures_quarantine),
    COUNT(*),
    (SELECT COUNT(*) FROM silver_enclosures_quarantine),
    AVG(area_sqm),
    NOW()
FROM silver_enclosures;

-- =============================
-- END OF CLEANING JOB 