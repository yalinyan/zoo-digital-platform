import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Optional
import pandas as pd
from loguru import logger
from .config import config

class ZooDatabase:
    """Database connector for zoo data across all layers."""
    
    def __init__(self):
        self.connection_params = {
            'host': config.database_host,
            'port': config.database_port,
            'database': config.database_name,
            'user': config.database_user,
            'password': config.database_password
        }
    
    def get_connection(self):
        """Get database connection."""
        return psycopg2.connect(**self.connection_params)
    
    def query_to_dataframe(self, query: str, params: tuple = None) -> pd.DataFrame:
        """Execute query and return results as DataFrame."""
        try:
            with self.get_connection() as conn:
                df = pd.read_sql_query(query, conn, params=params)
                return df
        except Exception as e:
            logger.error(f"Database query error: {e}")
            return pd.DataFrame()
    
    def get_animal_cost_history(self, animal_id: str = None, months: int = 6) -> pd.DataFrame:
        """Get historical cost data for animals."""
        query = """
            SELECT 
                animal_id,
                animal_name,
                species,
                date,
                feeding_cost_usd,
                medical_cost_usd,
                enclosure_maintenance_cost_usd,
                staff_time_cost_usd,
                total_daily_cost_usd
            FROM gold_animal_costs
            WHERE date >= CURRENT_DATE - INTERVAL '%s months'
            ORDER BY animal_id, date
        """
        if animal_id:
            query = query.replace("WHERE", f"WHERE animal_id = '{animal_id}' AND")
        
        return self.query_to_dataframe(query, (months,))
    
    def get_species_cost_history(self, species: str = None, months: int = 6) -> pd.DataFrame:
        """Get historical cost data for species."""
        query = """
            SELECT 
                species,
                date,
                animal_count,
                total_feeding_cost_usd,
                total_medical_cost_usd,
                total_maintenance_cost_usd,
                total_staff_cost_usd,
                total_daily_cost_usd,
                avg_cost_per_animal_usd
            FROM gold_species_costs
            WHERE date >= CURRENT_DATE - INTERVAL '%s months'
            ORDER BY species, date
        """
        if species:
            query = query.replace("WHERE", f"WHERE species = '{species}' AND")
        
        return self.query_to_dataframe(query, (months,))
    
    def get_revenue_history(self, months: int = 6) -> pd.DataFrame:
        """Get historical revenue data."""
        query = """
            SELECT 
                date,
                total_visitors,
                total_revenue_usd,
                avg_revenue_per_visitor,
                ticket_revenue_usd,
                food_revenue_usd,
                souvenir_revenue_usd,
                other_revenue_usd
            FROM gold_daily_revenue
            WHERE date >= CURRENT_DATE - INTERVAL '%s months'
            ORDER BY date
        """
        return self.query_to_dataframe(query, (months,))
    
    def get_performance_metrics(self, days: int = 30) -> pd.DataFrame:
        """Get recent performance metrics."""
        query = """
            SELECT 
                date,
                total_revenue_usd,
                total_cost_usd,
                total_profit_usd,
                profit_margin_percentage,
                total_visitors,
                revenue_per_visitor_usd,
                cost_per_visitor_usd,
                profit_per_visitor_usd,
                avg_visitor_satisfaction,
                weather_condition
            FROM gold_daily_performance
            WHERE date >= CURRENT_DATE - INTERVAL '%s days'
            ORDER BY date
        """
        return self.query_to_dataframe(query, (days,))
    
    def get_animal_roi_data(self, days: int = 30) -> pd.DataFrame:
        """Get animal ROI data."""
        query = """
            SELECT 
                animal_id,
                animal_name,
                species,
                date,
                daily_revenue_usd,
                daily_cost_usd,
                daily_profit_usd,
                daily_roi_percentage,
                visitor_attraction_count,
                avg_visitor_satisfaction
            FROM gold_animal_roi
            WHERE date >= CURRENT_DATE - INTERVAL '%s days'
            ORDER BY date, daily_roi_percentage DESC
        """
        return self.query_to_dataframe(query, (days,))
    
    def get_species_roi_data(self, days: int = 30) -> pd.DataFrame:
        """Get species ROI data."""
        query = """
            SELECT 
                species,
                date,
                animal_count,
                total_daily_revenue_usd,
                total_daily_cost_usd,
                total_daily_profit_usd,
                avg_daily_roi_percentage,
                total_visitor_attraction_count,
                avg_visitor_satisfaction,
                revenue_per_animal_usd,
                cost_per_animal_usd,
                profit_per_animal_usd
            FROM gold_species_roi
            WHERE date >= CURRENT_DATE - INTERVAL '%s days'
            ORDER BY date, avg_daily_roi_percentage DESC
        """
        return self.query_to_dataframe(query, (days,))
    
    def get_weather_data(self, days: int = 30) -> pd.DataFrame:
        """Get weather data for correlation analysis."""
        query = """
            SELECT 
                date,
                location,
                temperature_celsius,
                humidity_percent,
                condition,
                recorded_at
            FROM silver_weather
            WHERE date >= CURRENT_DATE - INTERVAL '%s days'
            ORDER BY recorded_at
        """
        return self.query_to_dataframe(query, (days,))
    
    def get_visitor_data(self, days: int = 30) -> pd.DataFrame:
        """Get visitor data for analysis."""
        query = """
            SELECT 
                visitor_id,
                ticket_type,
                entry_time,
                exit_time,
                age_group,
                group_size,
                total_spent_usd,
                satisfaction_rating,
                visit_duration_minutes
            FROM silver_visitors
            WHERE entry_time >= CURRENT_DATE - INTERVAL '%s days'
            AND data_quality_score >= 0.7
            ORDER BY entry_time
        """
        return self.query_to_dataframe(query, (days,))
    
    def get_feeding_data(self, days: int = 30) -> pd.DataFrame:
        """Get feeding data for cost analysis."""
        query = """
            SELECT 
                feeding_id,
                animal_id,
                food_type,
                quantity_kg,
                feeding_time,
                keeper_id,
                consumed_amount_kg,
                consumption_percentage
            FROM silver_feeding
            WHERE feeding_time >= CURRENT_DATE - INTERVAL '%s days'
            AND data_quality_score >= 0.7
            ORDER BY feeding_time
        """
        return self.query_to_dataframe(query, (days,))
    
    def get_sensor_data(self, days: int = 30) -> pd.DataFrame:
        """Get sensor data for environmental analysis."""
        query = """
            SELECT 
                sensor_id,
                sensor_type,
                enclosure_id,
                recorded_at,
                temperature_celsius,
                humidity_percent,
                air_quality_index,
                battery_level_percent
            FROM silver_sensors
            WHERE recorded_at >= CURRENT_DATE - INTERVAL '%s days'
            AND data_quality_score >= 0.7
            ORDER BY recorded_at
        """
        return self.query_to_dataframe(query, (days,))
    
    def get_cost_savings_opportunities(self) -> pd.DataFrame:
        """Get potential cost savings opportunities."""
        query = """
            SELECT 
                animal_id,
                animal_name,
                species,
                AVG(total_daily_cost_usd) as avg_daily_cost,
                AVG(feeding_cost_usd) as avg_feeding_cost,
                AVG(medical_cost_usd) as avg_medical_cost,
                AVG(enclosure_maintenance_cost_usd) as avg_maintenance_cost,
                AVG(staff_time_cost_usd) as avg_staff_cost,
                COUNT(*) as days_analyzed
            FROM gold_animal_costs
            WHERE date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY animal_id, animal_name, species
            HAVING AVG(total_daily_cost_usd) > (
                SELECT AVG(total_daily_cost_usd) * 1.2 
                FROM gold_animal_costs 
                WHERE date >= CURRENT_DATE - INTERVAL '30 days'
            )
            ORDER BY avg_daily_cost DESC
        """
        return self.query_to_dataframe(query)
    
    def get_revenue_opportunities(self) -> pd.DataFrame:
        """Get potential revenue increase opportunities."""
        query = """
            SELECT 
                animal_id,
                animal_name,
                species,
                AVG(daily_revenue_usd) as avg_daily_revenue,
                AVG(visitor_attraction_count) as avg_visitors,
                AVG(avg_visitor_satisfaction) as avg_satisfaction,
                AVG(daily_roi_percentage) as avg_roi,
                COUNT(*) as days_analyzed
            FROM gold_animal_roi
            WHERE date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY animal_id, animal_name, species
            HAVING AVG(daily_revenue_usd) < (
                SELECT AVG(daily_revenue_usd) * 0.8 
                FROM gold_animal_roi 
                WHERE date >= CURRENT_DATE - INTERVAL '30 days'
            )
            ORDER BY avg_daily_revenue ASC
        """
        return self.query_to_dataframe(query)
    
    def get_monthly_forecast_data(self) -> pd.DataFrame:
        """Get data for cost forecasting."""
        query = """
            SELECT 
                animal_id,
                animal_name,
                species,
                DATE_TRUNC('month', date) as month,
                AVG(total_daily_cost_usd) as avg_monthly_cost,
                AVG(feeding_cost_usd) as avg_feeding_cost,
                AVG(medical_cost_usd) as avg_medical_cost,
                AVG(enclosure_maintenance_cost_usd) as avg_maintenance_cost,
                AVG(staff_time_cost_usd) as avg_staff_cost,
                COUNT(*) as days_in_month
            FROM gold_animal_costs
            WHERE date >= CURRENT_DATE - INTERVAL '12 months'
            GROUP BY animal_id, animal_name, species, DATE_TRUNC('month', date)
            ORDER BY animal_id, month
        """
        return self.query_to_dataframe(query)
    
    def get_high_cost_animals(self) -> pd.DataFrame:
        """Get animals with high daily costs."""
        query = """
            SELECT 
                animal_id,
                animal_name,
                species,
                AVG(total_daily_cost_usd) as avg_daily_cost,
                AVG(feeding_cost_usd) as avg_feeding_cost,
                AVG(medical_cost_usd) as avg_medical_cost,
                AVG(enclosure_maintenance_cost_usd) as avg_maintenance_cost,
                AVG(staff_time_cost_usd) as avg_staff_cost
            FROM gold_animal_costs
            WHERE date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY animal_id, animal_name, species
            HAVING AVG(total_daily_cost_usd) > (
                SELECT AVG(total_daily_cost_usd) * 1.5 
                FROM gold_animal_costs 
                WHERE date >= CURRENT_DATE - INTERVAL '30 days'
            )
            ORDER BY avg_daily_cost DESC
            LIMIT 10
        """
        return self.query_to_dataframe(query)
    
    def get_feeding_efficiency_data(self) -> pd.DataFrame:
        """Get feeding efficiency data for cost analysis."""
        query = """
            SELECT 
                f.animal_id,
                a.animal_name,
                a.species,
                AVG(f.consumption_percentage) as consumption_percentage,
                AVG(f.quantity_kg) as quantity_kg,
                COUNT(*) as feeding_count
            FROM silver_feeding f
            JOIN silver_animals a ON f.animal_id = a.animal_id
            WHERE f.feeding_time >= CURRENT_DATE - INTERVAL '30 days'
            AND f.data_quality_score >= 0.7
            GROUP BY f.animal_id, a.animal_name, a.species
            HAVING AVG(f.consumption_percentage) < 80
            ORDER BY consumption_percentage ASC
        """
        return self.query_to_dataframe(query)
    
    def get_species_cost_comparison(self) -> pd.DataFrame:
        """Get cost comparison across species."""
        query = """
            SELECT 
                species,
                COUNT(DISTINCT animal_id) as total_animals,
                AVG(total_daily_cost_usd) as avg_cost_per_animal,
                SUM(total_daily_cost_usd) as total_species_cost,
                AVG(feeding_cost_usd) as avg_feeding_cost,
                AVG(medical_cost_usd) as avg_medical_cost,
                AVG(enclosure_maintenance_cost_usd) as avg_maintenance_cost,
                AVG(staff_time_cost_usd) as avg_staff_cost
            FROM gold_animal_costs
            WHERE date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY species
            HAVING COUNT(DISTINCT animal_id) >= 2
            ORDER BY avg_cost_per_animal DESC
        """
        return self.query_to_dataframe(query)
    
    def get_seasonal_cost_patterns(self) -> pd.DataFrame:
        """Get seasonal cost patterns."""
        query = """
            SELECT 
                EXTRACT(MONTH FROM date) as month,
                AVG(total_daily_cost_usd) as avg_daily_cost,
                AVG(feeding_cost_usd) as avg_feeding_cost,
                AVG(medical_cost_usd) as avg_medical_cost,
                AVG(enclosure_maintenance_cost_usd) as avg_maintenance_cost,
                AVG(staff_time_cost_usd) as avg_staff_cost,
                COUNT(*) as days_analyzed
            FROM gold_animal_costs
            WHERE date >= CURRENT_DATE - INTERVAL '12 months'
            GROUP BY EXTRACT(MONTH FROM date)
            ORDER BY month
        """
        return self.query_to_dataframe(query)
    
    def get_low_performing_animals(self) -> pd.DataFrame:
        """Get animals with low revenue generation."""
        query = """
            SELECT 
                animal_id,
                animal_name,
                species,
                AVG(daily_revenue_usd) as avg_daily_revenue,
                AVG(visitor_attraction_count) as total_visitors_attracted,
                AVG(avg_visitor_satisfaction) as avg_satisfaction,
                AVG(daily_roi_percentage) as avg_roi
            FROM gold_animal_roi
            WHERE date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY animal_id, animal_name, species
            HAVING AVG(daily_revenue_usd) < (
                SELECT AVG(daily_revenue_usd) * 0.7 
                FROM gold_animal_roi 
                WHERE date >= CURRENT_DATE - INTERVAL '30 days'
            )
            ORDER BY avg_daily_revenue ASC
            LIMIT 10
        """
        return self.query_to_dataframe(query)
    
    def get_visitor_satisfaction_data(self) -> pd.DataFrame:
        """Get visitor satisfaction data by enclosure."""
        query = """
            SELECT 
                e.enclosure_id,
                e.enclosure_name,
                AVG(v.satisfaction_rating) as avg_satisfaction,
                COUNT(v.visitor_id) as visitor_count,
                SUM(v.total_spent_usd) as total_revenue
            FROM silver_visitors v
            JOIN silver_enclosures e ON v.enclosure_id = e.enclosure_id
            WHERE v.entry_time >= CURRENT_DATE - INTERVAL '30 days'
            AND v.data_quality_score >= 0.7
            GROUP BY e.enclosure_id, e.enclosure_name
            HAVING COUNT(v.visitor_id) >= 10
            ORDER BY avg_satisfaction ASC
        """
        return self.query_to_dataframe(query)
    
    def get_peak_hour_analysis(self) -> pd.DataFrame:
        """Get visitor patterns by hour."""
        query = """
            SELECT 
                EXTRACT(HOUR FROM entry_time) as hour,
                COUNT(visitor_id) as visitor_count,
                AVG(total_spent_usd) as avg_spend,
                AVG(satisfaction_rating) as avg_satisfaction
            FROM silver_visitors
            WHERE entry_time >= CURRENT_DATE - INTERVAL '30 days'
            AND data_quality_score >= 0.7
            GROUP BY EXTRACT(HOUR FROM entry_time)
            ORDER BY hour
        """
        return self.query_to_dataframe(query)
    
    def get_weather_revenue_correlation(self) -> pd.DataFrame:
        """Get weather-revenue correlation data."""
        query = """
            SELECT 
                w.condition,
                AVG(p.total_revenue_usd) as avg_revenue,
                AVG(p.total_visitors) as avg_visitors,
                COUNT(*) as days_analyzed
            FROM silver_weather w
            JOIN gold_daily_performance p ON w.date = p.date
            WHERE w.date >= CURRENT_DATE - INTERVAL '90 days'
            AND w.data_quality_score >= 0.7
            GROUP BY w.condition
            HAVING COUNT(*) >= 5
            ORDER BY avg_revenue DESC
        """
        return self.query_to_dataframe(query)
    
    def get_current_performance_metrics(self) -> pd.DataFrame:
        """Get current performance metrics."""
        query = """
            SELECT 
                date,
                total_revenue_usd,
                total_cost_usd,
                total_profit_usd,
                profit_margin_percentage,
                total_visitors,
                revenue_per_visitor_usd,
                cost_per_visitor_usd,
                profit_per_visitor_usd,
                avg_visitor_satisfaction,
                weather_condition
            FROM gold_daily_performance
            WHERE date >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY date DESC
            LIMIT 1
        """
        return self.query_to_dataframe(query) 