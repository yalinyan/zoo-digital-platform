-- Zoo Digital Platform - Gold Layer Schema
-- This schema defines the business intelligence and analytics tables for financial reporting and ROI analysis

-- =====================================================
-- REVENUE TABLES
-- =====================================================

-- Daily revenue summary
CREATE TABLE IF NOT EXISTS gold_daily_revenue (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    total_visitors INTEGER NOT NULL,
    total_revenue_usd DECIMAL(12,2) NOT NULL,
    avg_revenue_per_visitor DECIMAL(8,2) NOT NULL,
    ticket_revenue_usd DECIMAL(10,2) NOT NULL,
    food_revenue_usd DECIMAL(10,2) NOT NULL,
    souvenir_revenue_usd DECIMAL(10,2) NOT NULL,
    other_revenue_usd DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Revenue by visitor type
CREATE TABLE IF NOT EXISTS gold_revenue_by_type (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    visitor_type VARCHAR(30) NOT NULL,
    visitor_count INTEGER NOT NULL,
    total_revenue_usd DECIMAL(10,2) NOT NULL,
    avg_revenue_per_visitor DECIMAL(8,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, visitor_type)
);

-- Revenue by enclosure/attraction
CREATE TABLE IF NOT EXISTS gold_revenue_by_enclosure (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    enclosure_id VARCHAR(20) NOT NULL,
    enclosure_name VARCHAR(100) NOT NULL,
    visitor_count INTEGER NOT NULL,
    revenue_contribution_usd DECIMAL(10,2) NOT NULL,
    avg_visit_duration_minutes INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, enclosure_id)
);

-- =====================================================
-- COST TABLES
-- =====================================================

-- Daily operational costs
CREATE TABLE IF NOT EXISTS gold_daily_costs (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    total_operational_cost_usd DECIMAL(12,2) NOT NULL,
    animal_care_cost_usd DECIMAL(10,2) NOT NULL,
    staff_cost_usd DECIMAL(10,2) NOT NULL,
    utilities_cost_usd DECIMAL(10,2) NOT NULL,
    maintenance_cost_usd DECIMAL(10,2) NOT NULL,
    food_cost_usd DECIMAL(10,2) NOT NULL,
    medical_cost_usd DECIMAL(10,2) NOT NULL,
    other_cost_usd DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Animal-specific costs
CREATE TABLE IF NOT EXISTS gold_animal_costs (
    id BIGSERIAL PRIMARY KEY,
    animal_id VARCHAR(50) NOT NULL,
    animal_name VARCHAR(100) NOT NULL,
    species VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    feeding_cost_usd DECIMAL(8,2) NOT NULL,
    medical_cost_usd DECIMAL(8,2) NOT NULL,
    enclosure_maintenance_cost_usd DECIMAL(8,2) NOT NULL,
    staff_time_cost_usd DECIMAL(8,2) NOT NULL,
    total_daily_cost_usd DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(animal_id, date)
);

-- Species cost analysis
CREATE TABLE IF NOT EXISTS gold_species_costs (
    id BIGSERIAL PRIMARY KEY,
    species VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    animal_count INTEGER NOT NULL,
    total_feeding_cost_usd DECIMAL(10,2) NOT NULL,
    total_medical_cost_usd DECIMAL(10,2) NOT NULL,
    total_maintenance_cost_usd DECIMAL(10,2) NOT NULL,
    total_staff_cost_usd DECIMAL(10,2) NOT NULL,
    total_daily_cost_usd DECIMAL(12,2) NOT NULL,
    avg_cost_per_animal_usd DECIMAL(8,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(species, date)
);

-- =====================================================
-- ROI ANALYSIS TABLES
-- =====================================================

-- Animal ROI analysis
CREATE TABLE IF NOT EXISTS gold_animal_roi (
    id BIGSERIAL PRIMARY KEY,
    animal_id VARCHAR(50) NOT NULL,
    animal_name VARCHAR(100) NOT NULL,
    species VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    daily_revenue_usd DECIMAL(10,2) NOT NULL,
    daily_cost_usd DECIMAL(10,2) NOT NULL,
    daily_profit_usd DECIMAL(10,2) NOT NULL,
    daily_roi_percentage DECIMAL(5,2) NOT NULL,
    visitor_attraction_count INTEGER NOT NULL,
    avg_visitor_satisfaction DECIMAL(3,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(animal_id, date)
);

-- Species ROI analysis
CREATE TABLE IF NOT EXISTS gold_species_roi (
    id BIGSERIAL PRIMARY KEY,
    species VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    animal_count INTEGER NOT NULL,
    total_daily_revenue_usd DECIMAL(12,2) NOT NULL,
    total_daily_cost_usd DECIMAL(12,2) NOT NULL,
    total_daily_profit_usd DECIMAL(12,2) NOT NULL,
    avg_daily_roi_percentage DECIMAL(5,2) NOT NULL,
    total_visitor_attraction_count INTEGER NOT NULL,
    avg_visitor_satisfaction DECIMAL(3,2),
    revenue_per_animal_usd DECIMAL(8,2) NOT NULL,
    cost_per_animal_usd DECIMAL(8,2) NOT NULL,
    profit_per_animal_usd DECIMAL(8,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(species, date)
);

-- =====================================================
-- PROJECTION VS ACTUAL TABLES
-- =====================================================

-- Revenue projections vs actual
CREATE TABLE IF NOT EXISTS gold_revenue_projection_vs_actual (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    projected_revenue_usd DECIMAL(12,2) NOT NULL,
    actual_revenue_usd DECIMAL(12,2) NOT NULL,
    variance_usd DECIMAL(12,2) NOT NULL,
    variance_percentage DECIMAL(5,2) NOT NULL,
    projected_visitors INTEGER NOT NULL,
    actual_visitors INTEGER NOT NULL,
    visitor_variance INTEGER NOT NULL,
    visitor_variance_percentage DECIMAL(5,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date)
);

-- Cost projections vs actual
CREATE TABLE IF NOT EXISTS gold_cost_projection_vs_actual (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    projected_cost_usd DECIMAL(12,2) NOT NULL,
    actual_cost_usd DECIMAL(12,2) NOT NULL,
    variance_usd DECIMAL(12,2) NOT NULL,
    variance_percentage DECIMAL(5,2) NOT NULL,
    cost_category VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, cost_category)
);

-- =====================================================
-- PERFORMANCE METRICS TABLES
-- =====================================================

-- Daily performance metrics
CREATE TABLE IF NOT EXISTS gold_daily_performance (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    total_revenue_usd DECIMAL(12,2) NOT NULL,
    total_cost_usd DECIMAL(12,2) NOT NULL,
    total_profit_usd DECIMAL(12,2) NOT NULL,
    profit_margin_percentage DECIMAL(5,2) NOT NULL,
    total_visitors INTEGER NOT NULL,
    revenue_per_visitor_usd DECIMAL(8,2) NOT NULL,
    cost_per_visitor_usd DECIMAL(8,2) NOT NULL,
    profit_per_visitor_usd DECIMAL(8,2) NOT NULL,
    avg_visitor_satisfaction DECIMAL(3,2),
    weather_condition VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Monthly performance summary
CREATE TABLE IF NOT EXISTS gold_monthly_performance (
    id BIGSERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    total_revenue_usd DECIMAL(14,2) NOT NULL,
    total_cost_usd DECIMAL(14,2) NOT NULL,
    total_profit_usd DECIMAL(14,2) NOT NULL,
    profit_margin_percentage DECIMAL(5,2) NOT NULL,
    total_visitors INTEGER NOT NULL,
    avg_daily_visitors INTEGER NOT NULL,
    best_performing_species VARCHAR(50),
    worst_performing_species VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(year, month)
);

-- =====================================================
-- INDEXES FOR PERFORMANCE
-- =====================================================

-- Revenue indexes
CREATE INDEX IF NOT EXISTS idx_gold_daily_revenue_date ON gold_daily_revenue(date);
CREATE INDEX IF NOT EXISTS idx_gold_revenue_by_type_date ON gold_revenue_by_type(date);
CREATE INDEX IF NOT EXISTS idx_gold_revenue_by_enclosure_date ON gold_revenue_by_enclosure(date);

-- Cost indexes
CREATE INDEX IF NOT EXISTS idx_gold_daily_costs_date ON gold_daily_costs(date);
CREATE INDEX IF NOT EXISTS idx_gold_animal_costs_animal_date ON gold_animal_costs(animal_id, date);
CREATE INDEX IF NOT EXISTS idx_gold_species_costs_species_date ON gold_species_costs(species, date);

-- ROI indexes
CREATE INDEX IF NOT EXISTS idx_gold_animal_roi_animal_date ON gold_animal_roi(animal_id, date);
CREATE INDEX IF NOT EXISTS idx_gold_animal_roi_species_date ON gold_animal_roi(species, date);
CREATE INDEX IF NOT EXISTS idx_gold_species_roi_species_date ON gold_species_roi(species, date);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_gold_daily_performance_date ON gold_daily_performance(date);
CREATE INDEX IF NOT EXISTS idx_gold_monthly_performance_year_month ON gold_monthly_performance(year, month);

-- =====================================================
-- ANALYTICAL VIEWS
-- =====================================================

-- Top performing animals view
CREATE OR REPLACE VIEW gold_top_performing_animals AS
SELECT 
    animal_id,
    animal_name,
    species,
    SUM(daily_revenue_usd) as total_revenue_usd,
    SUM(daily_cost_usd) as total_cost_usd,
    SUM(daily_profit_usd) as total_profit_usd,
    AVG(daily_roi_percentage) as avg_roi_percentage,
    SUM(visitor_attraction_count) as total_visitors_attracted,
    AVG(avg_visitor_satisfaction) as avg_satisfaction
FROM gold_animal_roi
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY animal_id, animal_name, species
ORDER BY total_profit_usd DESC;

-- Top performing species view
CREATE OR REPLACE VIEW gold_top_performing_species AS
SELECT 
    species,
    SUM(total_daily_revenue_usd) as total_revenue_usd,
    SUM(total_daily_cost_usd) as total_cost_usd,
    SUM(total_daily_profit_usd) as total_profit_usd,
    AVG(avg_daily_roi_percentage) as avg_roi_percentage,
    SUM(total_visitor_attraction_count) as total_visitors_attracted,
    AVG(avg_visitor_satisfaction) as avg_satisfaction,
    COUNT(DISTINCT date) as days_analyzed
FROM gold_species_roi
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY species
ORDER BY total_profit_usd DESC;

-- Revenue trend analysis view
CREATE OR REPLACE VIEW gold_revenue_trend_analysis AS
SELECT 
    date,
    total_revenue_usd,
    LAG(total_revenue_usd) OVER (ORDER BY date) as prev_day_revenue,
    total_revenue_usd - LAG(total_revenue_usd) OVER (ORDER BY date) as revenue_change,
    CASE 
        WHEN LAG(total_revenue_usd) OVER (ORDER BY date) > 0 
        THEN ((total_revenue_usd - LAG(total_revenue_usd) OVER (ORDER BY date)) / LAG(total_revenue_usd) OVER (ORDER BY date)) * 100
        ELSE 0 
    END as revenue_change_percentage,
    total_visitors,
    avg_revenue_per_visitor
FROM gold_daily_revenue
ORDER BY date DESC;

-- Cost efficiency analysis view
CREATE OR REPLACE VIEW gold_cost_efficiency_analysis AS
SELECT 
    dc.date,
    dc.total_operational_cost_usd,
    dr.total_revenue_usd,
    dr.total_visitors,
    dc.total_operational_cost_usd / dr.total_visitors as cost_per_visitor,
    dr.total_revenue_usd / dc.total_operational_cost_usd as revenue_cost_ratio,
    (dr.total_revenue_usd - dc.total_operational_cost_usd) / dc.total_operational_cost_usd * 100 as profit_margin_percentage
FROM gold_daily_costs dc
JOIN gold_daily_revenue dr ON dc.date = dr.date
ORDER BY dc.date DESC;

-- =====================================================
-- TRIGGERS FOR AUTOMATIC UPDATES
-- =====================================================

-- Update updated_at timestamp trigger function (if not already exists)
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply triggers to tables with updated_at columns
CREATE TRIGGER update_gold_daily_revenue_updated_at 
    BEFORE UPDATE ON gold_daily_revenue 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_gold_daily_costs_updated_at 
    BEFORE UPDATE ON gold_daily_costs 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_gold_daily_performance_updated_at 
    BEFORE UPDATE ON gold_daily_performance 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column(); 