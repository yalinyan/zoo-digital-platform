from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from data.gold.processor import GoldLayerProcessor

# Default arguments for the DAG
default_args = {
    'owner': 'zoo-data-team',
    'depends_on_past': True,  # Wait for previous day's data
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

# Define the DAG
dag = DAG(
    'gold_layer_processing',
    default_args=default_args,
    description='Daily gold layer processing for business intelligence and ROI analysis',
    schedule_interval='0 3 * * *',  # Run daily at 3 AM (after silver layer processing)
    catchup=False,
    tags=['zoo', 'gold-layer', 'business-intelligence', 'roi-analysis'],
)

def process_gold_layer(**context):
    """Process gold layer data for the execution date."""
    execution_date = context['execution_date'].date()
    
    # Initialize processor
    processor = GoldLayerProcessor()
    
    # Process the day
    results = processor.process_full_day(execution_date)
    
    # Log results
    print(f"Gold layer processing completed for {execution_date}")
    print(f"Results: {results}")
    
    return results

def validate_gold_data(**context):
    """Validate that gold layer processing was successful."""
    execution_date = context['execution_date'].date()
    
    processor = GoldLayerProcessor()
    
    with processor.get_connection() as conn:
        with conn.cursor() as cur:
            # Check if revenue data exists
            cur.execute("""
                SELECT COUNT(*) as revenue_count
                FROM gold_daily_revenue
                WHERE date = %s
            """, (execution_date,))
            
            revenue_count = cur.fetchone()[0]
            
            # Check if performance data exists
            cur.execute("""
                SELECT COUNT(*) as performance_count
                FROM gold_daily_performance
                WHERE date = %s
            """, (execution_date,))
            
            performance_count = cur.fetchone()[0]
            
            # Check if ROI data exists
            cur.execute("""
                SELECT COUNT(*) as roi_count
                FROM gold_animal_roi
                WHERE date = %s
            """, (execution_date,))
            
            roi_count = cur.fetchone()[0]
    
    if revenue_count == 0 or performance_count == 0 or roi_count == 0:
        raise ValueError(f"Gold layer processing incomplete for {execution_date}")
    
    print(f"Gold layer validation passed for {execution_date}")
    print(f"Revenue records: {revenue_count}")
    print(f"Performance records: {performance_count}")
    print(f"ROI records: {roi_count}")
    
    return {
        'revenue_records': revenue_count,
        'performance_records': performance_count,
        'roi_records': roi_count
    }

def generate_monthly_summary(**context):
    """Generate monthly performance summary."""
    execution_date = context['execution_date'].date()
    year = execution_date.year
    month = execution_date.month
    
    processor = GoldLayerProcessor()
    
    with processor.get_connection() as conn:
        with conn.cursor() as cur:
            # Calculate monthly metrics
            cur.execute("""
                INSERT INTO gold_monthly_performance 
                (year, month, total_revenue_usd, total_cost_usd, total_profit_usd, 
                 profit_margin_percentage, total_visitors, avg_daily_visitors,
                 best_performing_species, worst_performing_species)
                SELECT 
                    %s as year,
                    %s as month,
                    SUM(total_revenue_usd) as total_revenue_usd,
                    SUM(total_cost_usd) as total_cost_usd,
                    SUM(total_profit_usd) as total_profit_usd,
                    AVG(profit_margin_percentage) as profit_margin_percentage,
                    SUM(total_visitors) as total_visitors,
                    AVG(total_visitors) as avg_daily_visitors,
                    (SELECT species FROM gold_species_roi 
                     WHERE DATE_TRUNC('month', date) = DATE_TRUNC('month', %s::date)
                     GROUP BY species ORDER BY SUM(total_daily_profit_usd) DESC LIMIT 1) as best_species,
                    (SELECT species FROM gold_species_roi 
                     WHERE DATE_TRUNC('month', date) = DATE_TRUNC('month', %s::date)
                     GROUP BY species ORDER BY SUM(total_daily_profit_usd) ASC LIMIT 1) as worst_species
                FROM gold_daily_performance
                WHERE DATE_TRUNC('month', date) = DATE_TRUNC('month', %s::date)
                ON CONFLICT (year, month) DO UPDATE SET
                    total_revenue_usd = EXCLUDED.total_revenue_usd,
                    total_cost_usd = EXCLUDED.total_cost_usd,
                    total_profit_usd = EXCLUDED.total_profit_usd,
                    profit_margin_percentage = EXCLUDED.profit_margin_percentage,
                    total_visitors = EXCLUDED.total_visitors,
                    avg_daily_visitors = EXCLUDED.avg_daily_visitors,
                    best_performing_species = EXCLUDED.best_performing_species,
                    worst_performing_species = EXCLUDED.worst_performing_species
            """, (year, month, execution_date, execution_date, execution_date))
            
            conn.commit()
    
    print(f"Monthly summary generated for {year}-{month:02d}")
    return {'year': year, 'month': month}

# Define tasks
process_gold_task = PythonOperator(
    task_id='process_gold_layer',
    python_callable=process_gold_layer,
    provide_context=True,
    dag=dag,
)

validate_gold_task = PythonOperator(
    task_id='validate_gold_data',
    python_callable=validate_gold_data,
    provide_context=True,
    dag=dag,
)

monthly_summary_task = PythonOperator(
    task_id='generate_monthly_summary',
    python_callable=generate_monthly_summary,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
process_gold_task >> validate_gold_task >> monthly_summary_task 