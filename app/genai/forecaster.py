import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, date, timedelta
from loguru import logger
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error
import warnings
warnings.filterwarnings('ignore')

from .database import ZooDatabase
from .config import config

class CostForecaster:
    """Cost forecasting module for zoo animals and species."""
    
    def __init__(self):
        self.db = ZooDatabase()
        self.scaler = StandardScaler()
        
    def prepare_forecast_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare data for forecasting with time features."""
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        # Add time features
        df['day_of_week'] = df['date'].dt.dayofweek
        df['day_of_month'] = df['date'].dt.day
        df['month'] = df['date'].dt.month
        df['quarter'] = df['date'].dt.quarter
        df['year'] = df['date'].dt.year
        
        # Add lag features
        df['cost_lag_1'] = df['total_daily_cost_usd'].shift(1)
        df['cost_lag_7'] = df['total_daily_cost_usd'].shift(7)
        df['cost_lag_30'] = df['total_daily_cost_usd'].shift(30)
        
        # Add rolling statistics
        df['cost_rolling_mean_7'] = df['total_daily_cost_usd'].rolling(window=7).mean()
        df['cost_rolling_std_7'] = df['total_daily_cost_usd'].rolling(window=7).std()
        df['cost_rolling_mean_30'] = df['total_daily_cost_usd'].rolling(window=30).mean()
        
        # Add trend features
        df['cost_trend'] = df['total_daily_cost_usd'].diff()
        df['cost_trend_7'] = df['total_daily_cost_usd'].diff(7)
        
        return df.dropna()
    
    def forecast_animal_costs(self, animal_id: str, forecast_months: int = 3) -> Dict[str, Any]:
        """Forecast costs for a specific animal."""
        try:
            # Get historical data
            df = self.db.get_animal_cost_history(animal_id, months=12)
            
            if df.empty:
                return {"error": f"No data found for animal {animal_id}"}
            
            # Prepare data
            df = self.prepare_forecast_data(df)
            
            if len(df) < 30:  # Need at least 30 days of data
                return {"error": f"Insufficient data for animal {animal_id}"}
            
            # Features for forecasting
            feature_columns = [
                'day_of_week', 'day_of_month', 'month', 'quarter', 'year',
                'cost_lag_1', 'cost_lag_7', 'cost_lag_30',
                'cost_rolling_mean_7', 'cost_rolling_std_7', 'cost_rolling_mean_30',
                'cost_trend', 'cost_trend_7'
            ]
            
            X = df[feature_columns].values
            y = df['total_daily_cost_usd'].values
            
            # Train model
            model = RandomForestRegressor(n_estimators=100, random_state=42)
            model.fit(X, y)
            
            # Generate future dates
            last_date = df['date'].max()
            future_dates = pd.date_range(start=last_date + timedelta(days=1), 
                                       periods=forecast_months * 30, freq='D')
            
            # Prepare future features
            future_df = pd.DataFrame({'date': future_dates})
            future_df['day_of_week'] = future_df['date'].dt.dayofweek
            future_df['day_of_month'] = future_df['date'].dt.day
            future_df['month'] = future_df['date'].dt.month
            future_df['quarter'] = future_df['date'].dt.quarter
            future_df['year'] = future_df['date'].dt.year
            
            # Initialize lag features with last known values
            last_costs = df['total_daily_cost_usd'].tail(30).values
            
            forecasts = []
            for i, row in future_df.iterrows():
                # Create feature vector for this date
                features = [row[col] for col in feature_columns[:5]]  # Time features
                
                # Add lag features (use last known values initially)
                if i < 1:
                    features.append(last_costs[-1])
                else:
                    features.append(forecasts[i-1])
                
                if i < 7:
                    features.append(last_costs[-7] if len(last_costs) >= 7 else last_costs[-1])
                else:
                    features.append(forecasts[i-7])
                
                if i < 30:
                    features.append(last_costs[-30] if len(last_costs) >= 30 else last_costs[-1])
                else:
                    features.append(forecasts[i-30])
                
                # Add rolling statistics
                if i < 7:
                    features.extend([df['cost_rolling_mean_7'].iloc[-1], 
                                   df['cost_rolling_std_7'].iloc[-1],
                                   df['cost_rolling_mean_30'].iloc[-1]])
                else:
                    recent_forecasts = forecasts[max(0, i-7):i]
                    features.extend([np.mean(recent_forecasts), np.std(recent_forecasts),
                                   np.mean(forecasts[max(0, i-30):i])])
                
                # Add trend features
                if i < 1:
                    features.extend([df['cost_trend'].iloc[-1], df['cost_trend_7'].iloc[-1]])
                else:
                    features.extend([forecasts[i] - forecasts[i-1],
                                   forecasts[i] - forecasts[max(0, i-7)]])
                
                # Make prediction
                prediction = model.predict([features])[0]
                forecasts.append(max(0, prediction))  # Ensure non-negative costs
            
            # Calculate confidence intervals (simplified)
            forecast_std = np.std(forecasts)
            confidence_interval = 1.96 * forecast_std  # 95% confidence
            
            # Aggregate by month
            future_df['forecasted_cost'] = forecasts
            monthly_forecast = future_df.groupby(future_df['date'].dt.to_period('M')).agg({
                'forecasted_cost': ['sum', 'mean', 'std']
            }).round(2)
            
            # Calculate total forecast
            total_forecast = sum(forecasts)
            avg_daily_forecast = np.mean(forecasts)
            
            return {
                "animal_id": animal_id,
                "animal_name": df['animal_name'].iloc[0],
                "species": df['species'].iloc[0],
                "forecast_months": forecast_months,
                "total_forecasted_cost": total_forecast,
                "avg_daily_forecast": avg_daily_forecast,
                "monthly_forecast": monthly_forecast.to_dict(),
                "confidence_interval": confidence_interval,
                "last_known_cost": df['total_daily_cost_usd'].iloc[-1],
                "cost_trend": "increasing" if avg_daily_forecast > df['total_daily_cost_usd'].iloc[-1] else "decreasing",
                "forecast_dates": future_dates.tolist(),
                "forecasted_costs": forecasts
            }
            
        except Exception as e:
            logger.error(f"Error forecasting costs for animal {animal_id}: {e}")
            return {"error": str(e)}
    
    def forecast_species_costs(self, species: str, forecast_months: int = 3) -> Dict[str, Any]:
        """Forecast costs for a specific species."""
        try:
            # Get historical data
            df = self.db.get_species_cost_history(species, months=12)
            
            if df.empty:
                return {"error": f"No data found for species {species}"}
            
            # Prepare data
            df = self.prepare_forecast_data(df)
            
            if len(df) < 30:
                return {"error": f"Insufficient data for species {species}"}
            
            # Features for forecasting
            feature_columns = [
                'day_of_week', 'day_of_month', 'month', 'quarter', 'year',
                'cost_lag_1', 'cost_lag_7', 'cost_lag_30',
                'cost_rolling_mean_7', 'cost_rolling_std_7', 'cost_rolling_mean_30',
                'cost_trend', 'cost_trend_7', 'animal_count'
            ]
            
            X = df[feature_columns].values
            y = df['total_daily_cost_usd'].values
            
            # Train model
            model = RandomForestRegressor(n_estimators=100, random_state=42)
            model.fit(X, y)
            
            # Generate future dates
            last_date = df['date'].max()
            future_dates = pd.date_range(start=last_date + timedelta(days=1), 
                                       periods=forecast_months * 30, freq='D')
            
            # Prepare future features
            future_df = pd.DataFrame({'date': future_dates})
            future_df['day_of_week'] = future_df['date'].dt.dayofweek
            future_df['day_of_month'] = future_df['date'].dt.day
            future_df['month'] = future_df['date'].dt.month
            future_df['quarter'] = future_df['date'].dt.quarter
            future_df['year'] = future_df['date'].dt.year
            future_df['animal_count'] = df['animal_count'].iloc[-1]  # Assume constant
            
            # Initialize lag features
            last_costs = df['total_daily_cost_usd'].tail(30).values
            
            forecasts = []
            for i, row in future_df.iterrows():
                features = [row[col] for col in feature_columns[:5]]
                features.append(row['animal_count'])
                
                # Add lag features
                if i < 1:
                    features.append(last_costs[-1])
                else:
                    features.append(forecasts[i-1])
                
                if i < 7:
                    features.append(last_costs[-7] if len(last_costs) >= 7 else last_costs[-1])
                else:
                    features.append(forecasts[i-7])
                
                if i < 30:
                    features.append(last_costs[-30] if len(last_costs) >= 30 else last_costs[-1])
                else:
                    features.append(forecasts[i-30])
                
                # Add rolling statistics
                if i < 7:
                    features.extend([df['cost_rolling_mean_7'].iloc[-1], 
                                   df['cost_rolling_std_7'].iloc[-1],
                                   df['cost_rolling_mean_30'].iloc[-1]])
                else:
                    recent_forecasts = forecasts[max(0, i-7):i]
                    features.extend([np.mean(recent_forecasts), np.std(recent_forecasts),
                                   np.mean(forecasts[max(0, i-30):i])])
                
                # Add trend features
                if i < 1:
                    features.extend([df['cost_trend'].iloc[-1], df['cost_trend_7'].iloc[-1]])
                else:
                    features.extend([forecasts[i] - forecasts[i-1],
                                   forecasts[i] - forecasts[max(0, i-7)]])
                
                # Make prediction
                prediction = model.predict([features])[0]
                forecasts.append(max(0, prediction))
            
            # Aggregate by month
            future_df['forecasted_cost'] = forecasts
            monthly_forecast = future_df.groupby(future_df['date'].dt.to_period('M')).agg({
                'forecasted_cost': ['sum', 'mean', 'std']
            }).round(2)
            
            total_forecast = sum(forecasts)
            avg_daily_forecast = np.mean(forecasts)
            
            return {
                "species": species,
                "forecast_months": forecast_months,
                "total_forecasted_cost": total_forecast,
                "avg_daily_forecast": avg_daily_forecast,
                "avg_cost_per_animal": avg_daily_forecast / df['animal_count'].iloc[-1],
                "monthly_forecast": monthly_forecast.to_dict(),
                "animal_count": df['animal_count'].iloc[-1],
                "last_known_cost": df['total_daily_cost_usd'].iloc[-1],
                "cost_trend": "increasing" if avg_daily_forecast > df['total_daily_cost_usd'].iloc[-1] else "decreasing",
                "forecast_dates": future_dates.tolist(),
                "forecasted_costs": forecasts
            }
            
        except Exception as e:
            logger.error(f"Error forecasting costs for species {species}: {e}")
            return {"error": str(e)}
    
    def generate_all_forecasts(self, forecast_months: int = 3) -> Dict[str, Any]:
        """Generate forecasts for all animals and species."""
        try:
            # Get all animals
            animals_df = self.db.query_to_dataframe("""
                SELECT DISTINCT animal_id, animal_name, species 
                FROM gold_animal_costs 
                WHERE date >= CURRENT_DATE - INTERVAL '30 days'
            """)
            
            # Get all species
            species_df = self.db.query_to_dataframe("""
                SELECT DISTINCT species 
                FROM gold_species_costs 
                WHERE date >= CURRENT_DATE - INTERVAL '30 days'
            """)
            
            animal_forecasts = {}
            species_forecasts = {}
            
            # Forecast for each animal
            for _, animal in animals_df.iterrows():
                forecast = self.forecast_animal_costs(animal['animal_id'], forecast_months)
                if 'error' not in forecast:
                    animal_forecasts[animal['animal_id']] = forecast
            
            # Forecast for each species
            for _, species_row in species_df.iterrows():
                forecast = self.forecast_species_costs(species_row['species'], forecast_months)
                if 'error' not in forecast:
                    species_forecasts[species_row['species']] = forecast
            
            # Calculate summary statistics
            total_animal_forecast = sum(f['total_forecasted_cost'] for f in animal_forecasts.values())
            total_species_forecast = sum(f['total_forecasted_cost'] for f in species_forecasts.values())
            
            return {
                "forecast_months": forecast_months,
                "total_animal_forecasts": len(animal_forecasts),
                "total_species_forecasts": len(species_forecasts),
                "total_animal_forecasted_cost": total_animal_forecast,
                "total_species_forecasted_cost": total_species_forecast,
                "animal_forecasts": animal_forecasts,
                "species_forecasts": species_forecasts,
                "generated_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error generating all forecasts: {e}")
            return {"error": str(e)} 