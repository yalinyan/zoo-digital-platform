from typing import Dict, List, Any, Optional
from datetime import datetime, date, timedelta
import pandas as pd
from loguru import logger
import psycopg2
from psycopg2.extras import RealDictCursor
from config.settings import get_settings

settings = get_settings()


class GoldLayerProcessor:
    """Processor for transforming silver layer data into gold layer business intelligence tables."""
    
    def __init__(self):
        self.db_config = {
            'host': settings.database.host,
            'port': settings.database.port,
            'database': settings.database.name,
            'user': settings.database.user,
            'password': settings.database.password
        }
    
    def get_connection(self):
        """Get database connection."""
        return psycopg2.connect(**self.db_config)
    
    def process_daily_revenue(self, target_date: date) -> Dict[str, Any]:
        """Calculate daily revenue from visitor data."""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get visitor data for the date
                cur.execute("""
                    SELECT 
                        COUNT(*) as total_visitors,
                        SUM(total_spent_usd) as total_revenue,
                        AVG(total_spent_usd) as avg_revenue_per_visitor,
                        SUM(CASE WHEN ticket_type = 'adult' THEN total_spent_usd * 0.7 ELSE 0 END) as ticket_revenue,
                        SUM(CASE WHEN ticket_type = 'child' THEN total_spent_usd * 0.6 ELSE 0 END) as food_revenue,
                        SUM(CASE WHEN ticket_type = 'senior' THEN total_spent_usd * 0.2 ELSE 0 END) as souvenir_revenue,
                        SUM(CASE WHEN ticket_type NOT IN ('adult', 'child', 'senior') THEN total_spent_usd * 0.1 ELSE 0 END) as other_revenue
                    FROM silver_visitors 
                    WHERE DATE(entry_time) = %s AND data_quality_score >= 0.7
                """, (target_date,))
                
                result = cur.fetchone()
                
                if result and result['total_visitors'] > 0:
                    # Insert into gold_daily_revenue
                    cur.execute("""
                        INSERT INTO gold_daily_revenue 
                        (date, total_visitors, total_revenue_usd, avg_revenue_per_visitor, 
                         ticket_revenue_usd, food_revenue_usd, souvenir_revenue_usd, other_revenue_usd)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (date) DO UPDATE SET
                            total_visitors = EXCLUDED.total_visitors,
                            total_revenue_usd = EXCLUDED.total_revenue_usd,
                            avg_revenue_per_visitor = EXCLUDED.avg_revenue_per_visitor,
                            ticket_revenue_usd = EXCLUDED.ticket_revenue_usd,
                            food_revenue_usd = EXCLUDED.food_revenue_usd,
                            souvenir_revenue_usd = EXCLUDED.souvenir_revenue_usd,
                            other_revenue_usd = EXCLUDED.other_revenue_usd,
                            updated_at = CURRENT_TIMESTAMP
                    """, (
                        target_date,
                        result['total_visitors'],
                        result['total_revenue'],
                        result['avg_revenue_per_visitor'],
                        result['ticket_revenue'],
                        result['food_revenue'],
                        result['souvenir_revenue'],
                        result['other_revenue']
                    ))
                    
                    conn.commit()
                    logger.info(f"Processed daily revenue for {target_date}: ${result['total_revenue']:.2f}")
                    return result
                
                return None
    
    def process_animal_costs(self, target_date: date) -> Dict[str, Any]:
        """Calculate animal-specific costs from feeding and sensor data."""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get animal feeding costs
                cur.execute("""
                    SELECT 
                        f.animal_id,
                        a.name as animal_name,
                        a.species,
                        SUM(f.quantity_kg * 15.0) as feeding_cost_usd,  -- $15/kg average food cost
                        COUNT(*) * 25.0 as medical_cost_usd,  -- $25 per feeding session for medical monitoring
                        COUNT(*) * 10.0 as enclosure_maintenance_cost_usd,  -- $10 per feeding for enclosure upkeep
                        COUNT(*) * 30.0 as staff_time_cost_usd  -- $30 per feeding for staff time
                    FROM silver_feeding f
                    JOIN silver_animals a ON f.animal_id = a.animal_id
                    WHERE DATE(f.feeding_time) = %s AND f.data_quality_score >= 0.7
                    GROUP BY f.animal_id, a.name, a.species
                """, (target_date,))
                
                feeding_costs = cur.fetchall()
                
                for cost in feeding_costs:
                    total_cost = (cost['feeding_cost_usd'] + cost['medical_cost_usd'] + 
                                cost['enclosure_maintenance_cost_usd'] + cost['staff_time_cost_usd'])
                    
                    # Insert into gold_animal_costs
                    cur.execute("""
                        INSERT INTO gold_animal_costs 
                        (animal_id, animal_name, species, date, feeding_cost_usd, medical_cost_usd,
                         enclosure_maintenance_cost_usd, staff_time_cost_usd, total_daily_cost_usd)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (animal_id, date) DO UPDATE SET
                            feeding_cost_usd = EXCLUDED.feeding_cost_usd,
                            medical_cost_usd = EXCLUDED.medical_cost_usd,
                            enclosure_maintenance_cost_usd = EXCLUDED.enclosure_maintenance_cost_usd,
                            staff_time_cost_usd = EXCLUDED.staff_time_cost_usd,
                            total_daily_cost_usd = EXCLUDED.total_daily_cost_usd
                    """, (
                        cost['animal_id'],
                        cost['animal_name'],
                        cost['species'],
                        target_date,
                        cost['feeding_cost_usd'],
                        cost['medical_cost_usd'],
                        cost['enclosure_maintenance_cost_usd'],
                        cost['staff_time_cost_usd'],
                        total_cost
                    ))
                
                conn.commit()
                logger.info(f"Processed animal costs for {target_date}: {len(feeding_costs)} animals")
                return {'processed_animals': len(feeding_costs)}
    
    def process_species_costs(self, target_date: date) -> Dict[str, Any]:
        """Aggregate costs by species."""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        species,
                        COUNT(DISTINCT animal_id) as animal_count,
                        SUM(feeding_cost_usd) as total_feeding_cost,
                        SUM(medical_cost_usd) as total_medical_cost,
                        SUM(enclosure_maintenance_cost_usd) as total_maintenance_cost,
                        SUM(staff_time_cost_usd) as total_staff_cost,
                        SUM(total_daily_cost_usd) as total_daily_cost,
                        AVG(total_daily_cost_usd) as avg_cost_per_animal
                    FROM gold_animal_costs
                    WHERE date = %s
                    GROUP BY species
                """, (target_date,))
                
                species_costs = cur.fetchall()
                
                for cost in species_costs:
                    cur.execute("""
                        INSERT INTO gold_species_costs 
                        (species, date, animal_count, total_feeding_cost_usd, total_medical_cost_usd,
                         total_maintenance_cost_usd, total_staff_cost_usd, total_daily_cost_usd, avg_cost_per_animal_usd)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (species, date) DO UPDATE SET
                            animal_count = EXCLUDED.animal_count,
                            total_feeding_cost_usd = EXCLUDED.total_feeding_cost_usd,
                            total_medical_cost_usd = EXCLUDED.total_medical_cost_usd,
                            total_maintenance_cost_usd = EXCLUDED.total_maintenance_cost_usd,
                            total_staff_cost_usd = EXCLUDED.total_staff_cost_usd,
                            total_daily_cost_usd = EXCLUDED.total_daily_cost_usd,
                            avg_cost_per_animal_usd = EXCLUDED.avg_cost_per_animal_usd
                    """, (
                        cost['species'],
                        target_date,
                        cost['animal_count'],
                        cost['total_feeding_cost'],
                        cost['total_medical_cost'],
                        cost['total_maintenance_cost'],
                        cost['total_staff_cost'],
                        cost['total_daily_cost'],
                        cost['avg_cost_per_animal']
                    ))
                
                conn.commit()
                logger.info(f"Processed species costs for {target_date}: {len(species_costs)} species")
                return {'processed_species': len(species_costs)}
    
    def calculate_animal_roi(self, target_date: date) -> Dict[str, Any]:
        """Calculate ROI for each animal."""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get animal revenue contribution (based on visitor attraction)
                cur.execute("""
                    SELECT 
                        a.animal_id,
                        a.name as animal_name,
                        a.species,
                        COUNT(DISTINCT v.visitor_id) as visitor_attraction_count,
                        AVG(v.satisfaction_rating) as avg_satisfaction,
                        COUNT(DISTINCT v.visitor_id) * 5.0 as daily_revenue_usd  -- $5 per visitor attracted
                    FROM silver_animals a
                    LEFT JOIN silver_visitors v ON DATE(v.entry_time) = %s
                    WHERE a.data_quality_score >= 0.7
                    GROUP BY a.animal_id, a.name, a.species
                """, (target_date,))
                
                animal_revenues = cur.fetchall()
                
                for revenue in animal_revenues:
                    # Get animal cost
                    cur.execute("""
                        SELECT total_daily_cost_usd 
                        FROM gold_animal_costs 
                        WHERE animal_id = %s AND date = %s
                    """, (revenue['animal_id'], target_date))
                    
                    cost_result = cur.fetchone()
                    daily_cost = cost_result['total_daily_cost_usd'] if cost_result else 0
                    
                    daily_revenue = revenue['daily_revenue_usd']
                    daily_profit = daily_revenue - daily_cost
                    daily_roi = (daily_profit / daily_cost * 100) if daily_cost > 0 else 0
                    
                    # Insert into gold_animal_roi
                    cur.execute("""
                        INSERT INTO gold_animal_roi 
                        (animal_id, animal_name, species, date, daily_revenue_usd, daily_cost_usd,
                         daily_profit_usd, daily_roi_percentage, visitor_attraction_count, avg_visitor_satisfaction)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (animal_id, date) DO UPDATE SET
                            daily_revenue_usd = EXCLUDED.daily_revenue_usd,
                            daily_cost_usd = EXCLUDED.daily_cost_usd,
                            daily_profit_usd = EXCLUDED.daily_profit_usd,
                            daily_roi_percentage = EXCLUDED.daily_roi_percentage,
                            visitor_attraction_count = EXCLUDED.visitor_attraction_count,
                            avg_visitor_satisfaction = EXCLUDED.avg_visitor_satisfaction
                    """, (
                        revenue['animal_id'],
                        revenue['animal_name'],
                        revenue['species'],
                        target_date,
                        daily_revenue,
                        daily_cost,
                        daily_profit,
                        daily_roi,
                        revenue['visitor_attraction_count'],
                        revenue['avg_satisfaction']
                    ))
                
                conn.commit()
                logger.info(f"Calculated animal ROI for {target_date}: {len(animal_revenues)} animals")
                return {'processed_animals': len(animal_revenues)}
    
    def calculate_species_roi(self, target_date: date) -> Dict[str, Any]:
        """Calculate ROI for each species."""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        species,
                        COUNT(DISTINCT animal_id) as animal_count,
                        SUM(daily_revenue_usd) as total_daily_revenue,
                        SUM(daily_cost_usd) as total_daily_cost,
                        SUM(daily_profit_usd) as total_daily_profit,
                        AVG(daily_roi_percentage) as avg_daily_roi,
                        SUM(visitor_attraction_count) as total_visitor_attraction,
                        AVG(avg_visitor_satisfaction) as avg_satisfaction,
                        SUM(daily_revenue_usd) / COUNT(DISTINCT animal_id) as revenue_per_animal,
                        SUM(daily_cost_usd) / COUNT(DISTINCT animal_id) as cost_per_animal,
                        SUM(daily_profit_usd) / COUNT(DISTINCT animal_id) as profit_per_animal
                    FROM gold_animal_roi
                    WHERE date = %s
                    GROUP BY species
                """, (target_date,))
                
                species_roi = cur.fetchall()
                
                for roi in species_roi:
                    cur.execute("""
                        INSERT INTO gold_species_roi 
                        (species, date, animal_count, total_daily_revenue_usd, total_daily_cost_usd,
                         total_daily_profit_usd, avg_daily_roi_percentage, total_visitor_attraction_count,
                         avg_visitor_satisfaction, revenue_per_animal_usd, cost_per_animal_usd, profit_per_animal_usd)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (species, date) DO UPDATE SET
                            animal_count = EXCLUDED.animal_count,
                            total_daily_revenue_usd = EXCLUDED.total_daily_revenue_usd,
                            total_daily_cost_usd = EXCLUDED.total_daily_cost_usd,
                            total_daily_profit_usd = EXCLUDED.total_daily_profit_usd,
                            avg_daily_roi_percentage = EXCLUDED.avg_daily_roi_percentage,
                            total_visitor_attraction_count = EXCLUDED.total_visitor_attraction_count,
                            avg_visitor_satisfaction = EXCLUDED.avg_visitor_satisfaction,
                            revenue_per_animal_usd = EXCLUDED.revenue_per_animal_usd,
                            cost_per_animal_usd = EXCLUDED.cost_per_animal_usd,
                            profit_per_animal_usd = EXCLUDED.profit_per_animal_usd
                    """, (
                        roi['species'],
                        target_date,
                        roi['animal_count'],
                        roi['total_daily_revenue'],
                        roi['total_daily_cost'],
                        roi['total_daily_profit'],
                        roi['avg_daily_roi'],
                        roi['total_visitor_attraction'],
                        roi['avg_satisfaction'],
                        roi['revenue_per_animal'],
                        roi['cost_per_animal'],
                        roi['profit_per_animal']
                    ))
                
                conn.commit()
                logger.info(f"Calculated species ROI for {target_date}: {len(species_roi)} species")
                return {'processed_species': len(species_roi)}
    
    def process_daily_performance(self, target_date: date) -> Dict[str, Any]:
        """Calculate overall daily performance metrics."""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get revenue and cost data
                cur.execute("""
                    SELECT 
                        dr.total_revenue_usd,
                        dr.total_visitors,
                        COALESCE(dc.total_operational_cost_usd, 0) as total_cost_usd,
                        w.condition as weather_condition
                    FROM gold_daily_revenue dr
                    LEFT JOIN gold_daily_costs dc ON dr.date = dc.date
                    LEFT JOIN silver_weather w ON dr.date = DATE(w.recorded_at)
                    WHERE dr.date = %s
                """, (target_date,))
                
                result = cur.fetchone()
                
                if result:
                    total_revenue = result['total_revenue_usd']
                    total_cost = result['total_cost_usd']
                    total_visitors = result['total_visitors']
                    total_profit = total_revenue - total_cost
                    profit_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0
                    revenue_per_visitor = total_revenue / total_visitors if total_visitors > 0 else 0
                    cost_per_visitor = total_cost / total_visitors if total_visitors > 0 else 0
                    profit_per_visitor = total_profit / total_visitors if total_visitors > 0 else 0
                    
                    # Get average satisfaction
                    cur.execute("""
                        SELECT AVG(satisfaction_rating) as avg_satisfaction
                        FROM silver_visitors
                        WHERE DATE(entry_time) = %s AND satisfaction_rating IS NOT NULL
                    """, (target_date,))
                    
                    satisfaction_result = cur.fetchone()
                    avg_satisfaction = satisfaction_result['avg_satisfaction'] if satisfaction_result else None
                    
                    # Insert into gold_daily_performance
                    cur.execute("""
                        INSERT INTO gold_daily_performance 
                        (date, total_revenue_usd, total_cost_usd, total_profit_usd, profit_margin_percentage,
                         total_visitors, revenue_per_visitor_usd, cost_per_visitor_usd, profit_per_visitor_usd,
                         avg_visitor_satisfaction, weather_condition)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (date) DO UPDATE SET
                            total_revenue_usd = EXCLUDED.total_revenue_usd,
                            total_cost_usd = EXCLUDED.total_cost_usd,
                            total_profit_usd = EXCLUDED.total_profit_usd,
                            profit_margin_percentage = EXCLUDED.profit_margin_percentage,
                            total_visitors = EXCLUDED.total_visitors,
                            revenue_per_visitor_usd = EXCLUDED.revenue_per_visitor_usd,
                            cost_per_visitor_usd = EXCLUDED.cost_per_visitor_usd,
                            profit_per_visitor_usd = EXCLUDED.profit_per_visitor_usd,
                            avg_visitor_satisfaction = EXCLUDED.avg_visitor_satisfaction,
                            weather_condition = EXCLUDED.weather_condition,
                            updated_at = CURRENT_TIMESTAMP
                    """, (
                        target_date,
                        total_revenue,
                        total_cost,
                        total_profit,
                        profit_margin,
                        total_visitors,
                        revenue_per_visitor,
                        cost_per_visitor,
                        profit_per_visitor,
                        avg_satisfaction,
                        result['weather_condition']
                    ))
                    
                    conn.commit()
                    logger.info(f"Processed daily performance for {target_date}: ${total_profit:.2f} profit")
                    return {
                        'total_revenue': total_revenue,
                        'total_cost': total_cost,
                        'total_profit': total_profit,
                        'profit_margin': profit_margin
                    }
                
                return None
    
    def process_full_day(self, target_date: date) -> Dict[str, Any]:
        """Process all gold layer calculations for a given date."""
        logger.info(f"Starting gold layer processing for {target_date}")
        
        results = {}
        
        # Process revenue
        revenue_result = self.process_daily_revenue(target_date)
        results['revenue'] = revenue_result
        
        # Process costs
        animal_costs_result = self.process_animal_costs(target_date)
        results['animal_costs'] = animal_costs_result
        
        species_costs_result = self.process_species_costs(target_date)
        results['species_costs'] = species_costs_result
        
        # Calculate ROI
        animal_roi_result = self.calculate_animal_roi(target_date)
        results['animal_roi'] = animal_roi_result
        
        species_roi_result = self.calculate_species_roi(target_date)
        results['species_roi'] = species_roi_result
        
        # Process performance
        performance_result = self.process_daily_performance(target_date)
        results['performance'] = performance_result
        
        logger.info(f"Completed gold layer processing for {target_date}")
        return results 