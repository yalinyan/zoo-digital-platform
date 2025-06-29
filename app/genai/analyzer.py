import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, date, timedelta
from loguru import logger

from .database import ZooDatabase
from .config import config

class DailyAnalyzer:
    """Daily analysis module for cost savings and revenue opportunities."""
    
    def __init__(self):
        self.db = ZooDatabase()
        
    def analyze_cost_savings(self) -> Dict[str, Any]:
        """Analyze data to identify cost saving opportunities."""
        try:
            opportunities = []
            
            # 1. High-cost animals analysis
            high_cost_animals = self.db.get_high_cost_animals()
            if not high_cost_animals.empty:
                for _, animal in high_cost_animals.iterrows():
                    opportunities.append({
                        "type": "high_cost_animal",
                        "priority": "high",
                        "title": f"High Cost Animal: {animal['animal_name']}",
                        "description": f"{animal['animal_name']} ({animal['species']}) has average daily cost of ${animal['avg_daily_cost']:.2f}",
                        "potential_savings": animal['avg_daily_cost'] * 0.15,  # 15% potential savings
                        "recommendations": [
                            "Review feeding schedule and quantities",
                            "Optimize medical care protocols",
                            "Consider alternative food sources",
                            "Evaluate enclosure maintenance frequency"
                        ],
                        "data_points": {
                            "animal_id": animal['animal_id'],
                            "current_cost": animal['avg_daily_cost'],
                            "cost_breakdown": {
                                "feeding": animal['avg_feeding_cost'],
                                "medical": animal['avg_medical_cost'],
                                "maintenance": animal['avg_maintenance_cost'],
                                "staff": animal['avg_staff_cost']
                            }
                        }
                    })
            
            # 2. Inefficient feeding patterns
            feeding_analysis = self.db.get_feeding_efficiency_data()
            if not feeding_analysis.empty:
                low_efficiency = feeding_analysis[feeding_analysis['consumption_percentage'] < 70]
                for _, feeding in low_efficiency.iterrows():
                    opportunities.append({
                        "type": "feeding_inefficiency",
                        "priority": "medium",
                        "title": f"Low Food Consumption: {feeding['animal_name']}",
                        "description": f"Only {feeding['consumption_percentage']:.1f}% of food is consumed",
                        "potential_savings": feeding['quantity_kg'] * 0.3 * 15,  # 30% waste reduction
                        "recommendations": [
                            "Reduce food quantity by 20-30%",
                            "Adjust feeding schedule",
                            "Monitor food preferences",
                            "Consider different food types"
                        ],
                        "data_points": {
                            "animal_id": feeding['animal_id'],
                            "consumption_rate": feeding['consumption_percentage'],
                            "food_waste": feeding['quantity_kg'] * (1 - feeding['consumption_percentage'] / 100)
                        }
                    })
            
            # 3. Species cost comparison
            species_costs = self.db.get_species_cost_comparison()
            if not species_costs.empty:
                high_cost_species = species_costs[species_costs['avg_cost_per_animal'] > species_costs['avg_cost_per_animal'].mean() * 1.5]
                for _, species in high_cost_species.iterrows():
                    opportunities.append({
                        "type": "high_cost_species",
                        "priority": "medium",
                        "title": f"High Cost Species: {species['species']}",
                        "description": f"Average cost per {species['species']} is ${species['avg_cost_per_animal']:.2f}",
                        "potential_savings": species['total_animals'] * species['avg_cost_per_animal'] * 0.1,
                        "recommendations": [
                            "Review care protocols for the species",
                            "Optimize enclosure design",
                            "Consider group feeding strategies",
                            "Evaluate medical care efficiency"
                        ],
                        "data_points": {
                            "species": species['species'],
                            "animal_count": species['total_animals'],
                            "avg_cost": species['avg_cost_per_animal']
                        }
                    })
            
            # 4. Seasonal cost patterns
            seasonal_analysis = self.db.get_seasonal_cost_patterns()
            if not seasonal_analysis.empty:
                current_month = datetime.now().month
                current_costs = seasonal_analysis[seasonal_analysis['month'] == current_month]
                if not current_costs.empty:
                    avg_cost = current_costs['avg_daily_cost'].iloc[0]
                    if avg_cost > seasonal_analysis['avg_daily_cost'].mean() * 1.2:
                        opportunities.append({
                            "type": "seasonal_cost_spike",
                            "priority": "medium",
                            "title": "Seasonal Cost Increase",
                            "description": f"Current month costs are {((avg_cost / seasonal_analysis['avg_daily_cost'].mean()) - 1) * 100:.1f}% above average",
                            "potential_savings": avg_cost * 0.1,
                            "recommendations": [
                                "Review seasonal care requirements",
                                "Optimize heating/cooling systems",
                                "Adjust feeding for seasonal needs",
                                "Plan for seasonal cost variations"
                            ],
                            "data_points": {
                                "current_month": current_month,
                                "current_cost": avg_cost,
                                "average_cost": seasonal_analysis['avg_daily_cost'].mean()
                            }
                        })
            
            # Calculate total potential savings
            total_potential_savings = sum(opp['potential_savings'] for opp in opportunities)
            
            return {
                "analysis_date": datetime.now().isoformat(),
                "total_opportunities": len(opportunities),
                "total_potential_savings": total_potential_savings,
                "opportunities": opportunities,
                "summary": {
                    "high_priority": len([o for o in opportunities if o['priority'] == 'high']),
                    "medium_priority": len([o for o in opportunities if o['priority'] == 'medium']),
                    "low_priority": len([o for o in opportunities if o['priority'] == 'low'])
                }
            }
            
        except Exception as e:
            logger.error(f"Error analyzing cost savings: {e}")
            return {"error": str(e)}
    
    def analyze_revenue_opportunities(self) -> Dict[str, Any]:
        """Analyze data to identify revenue increase opportunities."""
        try:
            opportunities = []
            
            # 1. Low-performing animals
            low_performing_animals = self.db.get_low_performing_animals()
            if not low_performing_animals.empty:
                for _, animal in low_performing_animals.iterrows():
                    opportunities.append({
                        "type": "low_performing_animal",
                        "priority": "high",
                        "title": f"Low Revenue Animal: {animal['animal_name']}",
                        "description": f"{animal['animal_name']} generates only ${animal['avg_daily_revenue']:.2f} daily",
                        "potential_increase": animal['avg_daily_revenue'] * 0.5,  # 50% potential increase
                        "recommendations": [
                            "Improve enclosure visibility and accessibility",
                            "Add educational signage and information",
                            "Schedule feeding times during peak visitor hours",
                            "Create special viewing experiences",
                            "Promote the animal in marketing materials"
                        ],
                        "data_points": {
                            "animal_id": animal['animal_id'],
                            "current_revenue": animal['avg_daily_revenue'],
                            "visitor_attraction": animal['total_visitors_attracted'],
                            "satisfaction": animal['avg_satisfaction']
                        }
                    })
            
            # 2. Visitor satisfaction analysis
            satisfaction_analysis = self.db.get_visitor_satisfaction_data()
            if not satisfaction_analysis.empty:
                low_satisfaction = satisfaction_analysis[satisfaction_analysis['avg_satisfaction'] < 3.5]
                for _, area in low_satisfaction.iterrows():
                    opportunities.append({
                        "type": "low_satisfaction_area",
                        "priority": "medium",
                        "title": f"Low Satisfaction: {area['enclosure_name']}",
                        "description": f"Average satisfaction is {area['avg_satisfaction']:.1f}/5",
                        "potential_increase": area['total_revenue'] * 0.2,  # 20% potential increase
                        "recommendations": [
                            "Improve enclosure design and viewing areas",
                            "Add interactive elements and educational content",
                            "Enhance visitor experience with guided tours",
                            "Implement feedback collection system",
                            "Train staff on visitor engagement"
                        ],
                        "data_points": {
                            "enclosure_id": area['enclosure_id'],
                            "current_satisfaction": area['avg_satisfaction'],
                            "visitor_count": area['visitor_count'],
                            "revenue": area['total_revenue']
                        }
                    })
            
            # 3. Peak hour optimization
            peak_analysis = self.db.get_peak_hour_analysis()
            if not peak_analysis.empty:
                off_peak_hours = peak_analysis[peak_analysis['visitor_count'] < peak_analysis['visitor_count'].mean() * 0.7]
                for _, hour in off_peak_hours.iterrows():
                    opportunities.append({
                        "type": "off_peak_optimization",
                        "priority": "medium",
                        "title": f"Off-Peak Hour Optimization: {hour['hour']}:00",
                        "description": f"Only {hour['visitor_count']} visitors during {hour['hour']}:00",
                        "potential_increase": hour['visitor_count'] * 5 * 0.3,  # 30% more visitors
                        "recommendations": [
                            "Schedule special events during off-peak hours",
                            "Offer discounted tickets for off-peak times",
                            "Create themed experiences for specific hours",
                            "Promote off-peak visits in marketing",
                            "Schedule animal activities during low-traffic times"
                        ],
                        "data_points": {
                            "hour": hour['hour'],
                            "current_visitors": hour['visitor_count'],
                            "average_visitors": peak_analysis['visitor_count'].mean()
                        }
                    })
            
            # 4. Weather-based opportunities
            weather_analysis = self.db.get_weather_revenue_correlation()
            if not weather_analysis.empty:
                good_weather_days = weather_analysis[weather_analysis['avg_revenue'] > weather_analysis['avg_revenue'].mean()]
                if not good_weather_days.empty:
                    best_weather = good_weather_days.loc[good_weather_days['avg_revenue'].idxmax()]
                    opportunities.append({
                        "type": "weather_optimization",
                        "priority": "low",
                        "title": "Weather-Based Revenue Optimization",
                        "description": f"Best revenue on {best_weather['condition']} days (${best_weather['avg_revenue']:.2f})",
                        "potential_increase": best_weather['avg_revenue'] * 0.1,
                        "recommendations": [
                            "Plan special events for favorable weather conditions",
                            "Adjust pricing based on weather forecasts",
                            "Promote indoor/outdoor experiences based on weather",
                            "Optimize staffing for weather-dependent attendance"
                        ],
                        "data_points": {
                            "best_weather": best_weather['condition'],
                            "best_revenue": best_weather['avg_revenue'],
                            "average_revenue": weather_analysis['avg_revenue'].mean()
                        }
                    })
            
            # Calculate total potential increase
            total_potential_increase = sum(opp['potential_increase'] for opp in opportunities)
            
            return {
                "analysis_date": datetime.now().isoformat(),
                "total_opportunities": len(opportunities),
                "total_potential_increase": total_potential_increase,
                "opportunities": opportunities,
                "summary": {
                    "high_priority": len([o for o in opportunities if o['priority'] == 'high']),
                    "medium_priority": len([o for o in opportunities if o['priority'] == 'medium']),
                    "low_priority": len([o for o in opportunities if o['priority'] == 'low'])
                }
            }
            
        except Exception as e:
            logger.error(f"Error analyzing revenue opportunities: {e}")
            return {"error": str(e)}
    
    def generate_daily_report(self) -> Dict[str, Any]:
        """Generate comprehensive daily analysis report."""
        try:
            cost_analysis = self.analyze_cost_savings()
            revenue_analysis = self.analyze_revenue_opportunities()
            
            # Get current performance metrics
            current_performance = self.db.get_current_performance_metrics()
            
            report = {
                "report_date": datetime.now().isoformat(),
                "cost_savings_analysis": cost_analysis,
                "revenue_opportunities_analysis": revenue_analysis,
                "current_performance": current_performance,
                "summary": {
                    "total_cost_savings_opportunities": cost_analysis.get('total_opportunities', 0),
                    "total_revenue_opportunities": revenue_analysis.get('total_opportunities', 0),
                    "total_potential_savings": cost_analysis.get('total_potential_savings', 0),
                    "total_potential_revenue_increase": revenue_analysis.get('total_potential_increase', 0),
                    "net_potential_impact": (revenue_analysis.get('total_potential_increase', 0) - 
                                           cost_analysis.get('total_potential_savings', 0))
                }
            }
            
            # Add priority recommendations
            high_priority_items = []
            high_priority_items.extend([item for item in cost_analysis.get('opportunities', []) 
                                      if item['priority'] == 'high'])
            high_priority_items.extend([item for item in revenue_analysis.get('opportunities', []) 
                                      if item['priority'] == 'high'])
            
            report['high_priority_recommendations'] = high_priority_items[:5]  # Top 5
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating daily report: {e}")
            return {"error": str(e)}
    
    def analyze_detailed_cost_breakdown(self) -> Dict[str, Any]:
        """Analyze detailed cost breakdown by category."""
        try:
            # Get cost data for the last 30 days
            cost_data = self.db.get_animal_cost_history(months=1)
            
            if cost_data.empty:
                return {"error": "No cost data available"}
            
            # Calculate cost breakdown
            total_costs = {
                'feeding': cost_data['feeding_cost_usd'].sum(),
                'medical': cost_data['medical_cost_usd'].sum(),
                'maintenance': cost_data['enclosure_maintenance_cost_usd'].sum(),
                'staff': cost_data['staff_time_cost_usd'].sum()
            }
            
            total_cost = sum(total_costs.values())
            
            # Calculate percentages
            cost_percentages = {
                category: (amount / total_cost * 100) if total_cost > 0 else 0
                for category, amount in total_costs.items()
            }
            
            # Identify cost drivers
            cost_drivers = []
            for category, amount in total_costs.items():
                if cost_percentages[category] > 25:  # Categories over 25% of total
                    cost_drivers.append({
                        'category': category,
                        'amount': amount,
                        'percentage': cost_percentages[category],
                        'priority': 'high' if cost_percentages[category] > 40 else 'medium'
                    })
            
            # Get species cost comparison
            species_costs = self.db.get_species_cost_comparison()
            
            return {
                "analysis_date": datetime.now().isoformat(),
                "total_cost": total_cost,
                "cost_breakdown": total_costs,
                "cost_percentages": cost_percentages,
                "cost_drivers": cost_drivers,
                "species_cost_comparison": species_costs.to_dict('records') if not species_costs.empty else [],
                "recommendations": self._generate_cost_recommendations(cost_percentages, cost_drivers)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing detailed cost breakdown: {e}")
            return {"error": str(e)}
    
    def analyze_revenue_optimization_strategies(self) -> Dict[str, Any]:
        """Analyze revenue optimization strategies."""
        try:
            # Get visitor and revenue data
            visitor_data = self.db.get_visitor_data(days=30)
            performance_data = self.db.get_performance_metrics(days=30)
            
            if visitor_data.empty or performance_data.empty:
                return {"error": "Insufficient data for revenue optimization analysis"}
            
            strategies = []
            
            # 1. Pricing optimization
            avg_spend = visitor_data['total_spent_usd'].mean()
            ticket_types = visitor_data['ticket_type'].value_counts()
            
            if avg_spend < 25:  # Low average spend
                strategies.append({
                    "strategy": "pricing_optimization",
                    "title": "Optimize Pricing Strategy",
                    "description": f"Current average spend is ${avg_spend:.2f}. Consider premium experiences and upselling opportunities.",
                    "potential_impact": "15-25% revenue increase",
                    "implementation": "Medium term",
                    "priority": "high"
                })
            
            # 2. Peak hour optimization
            peak_analysis = self.db.get_peak_hour_analysis()
            if not peak_analysis.empty:
                peak_hours = peak_analysis[peak_analysis['visitor_count'] > peak_analysis['visitor_count'].mean()]
                off_peak_hours = peak_analysis[peak_analysis['visitor_count'] < peak_analysis['visitor_count'].mean() * 0.7]
                
                if len(off_peak_hours) > 0:
                    strategies.append({
                        "strategy": "peak_hour_optimization",
                        "title": "Optimize Off-Peak Hours",
                        "description": f"Found {len(off_peak_hours)} off-peak hours with low attendance.",
                        "potential_impact": "10-20% attendance increase",
                        "implementation": "Short term",
                        "priority": "medium"
                    })
            
            # 3. Visitor satisfaction improvement
            avg_satisfaction = visitor_data['satisfaction_rating'].mean()
            if avg_satisfaction < 4.0:
                strategies.append({
                    "strategy": "satisfaction_improvement",
                    "title": "Improve Visitor Satisfaction",
                    "description": f"Current satisfaction rating is {avg_satisfaction:.1f}/5. Focus on experience enhancement.",
                    "potential_impact": "20-30% repeat visitor increase",
                    "implementation": "Medium term",
                    "priority": "high"
                })
            
            # 4. Weather-based optimization
            weather_analysis = self.db.get_weather_revenue_correlation()
            if not weather_analysis.empty:
                best_weather = weather_analysis.iloc[0]
                strategies.append({
                    "strategy": "weather_optimization",
                    "title": "Weather-Based Revenue Optimization",
                    "description": f"Best revenue on {best_weather['condition']} days (${best_weather['avg_revenue']:.2f}).",
                    "potential_impact": "10-15% revenue increase",
                    "implementation": "Short term",
                    "priority": "medium"
                })
            
            return {
                "analysis_date": datetime.now().isoformat(),
                "total_strategies": len(strategies),
                "strategies": strategies,
                "current_metrics": {
                    "avg_spend": avg_spend,
                    "avg_satisfaction": avg_satisfaction,
                    "total_visitors": len(visitor_data)
                }
            }
            
        except Exception as e:
            logger.error(f"Error analyzing revenue optimization strategies: {e}")
            return {"error": str(e)}
    
    def analyze_animal_performance_trends(self) -> Dict[str, Any]:
        """Analyze animal performance trends over time."""
        try:
            # Get animal ROI data for the last 90 days
            animal_roi = self.db.get_animal_roi_data(days=90)
            
            if animal_roi.empty:
                return {"error": "No animal performance data available"}
            
            # Group by animal and calculate trends
            animal_trends = []
            
            for animal_id in animal_roi['animal_id'].unique():
                animal_data = animal_roi[animal_roi['animal_id'] == animal_id]
                
                if len(animal_data) >= 10:  # Need sufficient data points
                    # Calculate trend (simple linear regression)
                    x = np.arange(len(animal_data))
                    y_revenue = animal_data['daily_revenue_usd'].values
                    y_roi = animal_data['daily_roi_percentage'].values
                    
                    # Revenue trend
                    revenue_slope = np.polyfit(x, y_revenue, 1)[0]
                    revenue_trend = "increasing" if revenue_slope > 0.1 else "decreasing" if revenue_slope < -0.1 else "stable"
                    
                    # ROI trend
                    roi_slope = np.polyfit(x, y_roi, 1)[0]
                    roi_trend = "improving" if roi_slope > 0.5 else "declining" if roi_slope < -0.5 else "stable"
                    
                    animal_trends.append({
                        "animal_id": animal_id,
                        "animal_name": animal_data['animal_name'].iloc[0],
                        "species": animal_data['species'].iloc[0],
                        "avg_revenue": animal_data['daily_revenue_usd'].mean(),
                        "avg_roi": animal_data['daily_roi_percentage'].mean(),
                        "revenue_trend": revenue_trend,
                        "roi_trend": roi_trend,
                        "revenue_change_rate": revenue_slope,
                        "roi_change_rate": roi_slope,
                        "data_points": len(animal_data)
                    })
            
            # Sort by ROI trend (declining first)
            animal_trends.sort(key=lambda x: x['roi_change_rate'])
            
            # Identify concerning trends
            concerning_trends = [
                animal for animal in animal_trends 
                if animal['roi_trend'] == 'declining' or animal['revenue_trend'] == 'decreasing'
            ]
            
            return {
                "analysis_date": datetime.now().isoformat(),
                "total_animals_analyzed": len(animal_trends),
                "animal_trends": animal_trends,
                "concerning_trends": concerning_trends,
                "summary": {
                    "improving": len([a for a in animal_trends if a['roi_trend'] == 'improving']),
                    "stable": len([a for a in animal_trends if a['roi_trend'] == 'stable']),
                    "declining": len([a for a in animal_trends if a['roi_trend'] == 'declining'])
                }
            }
            
        except Exception as e:
            logger.error(f"Error analyzing animal performance trends: {e}")
            return {"error": str(e)}
    
    def generate_comprehensive_daily_analysis(self) -> Dict[str, Any]:
        """Generate comprehensive daily analysis including all aspects."""
        try:
            # Run all analyses
            cost_savings = self.analyze_cost_savings()
            revenue_opportunities = self.analyze_revenue_opportunities()
            cost_breakdown = self.analyze_detailed_cost_breakdown()
            revenue_strategies = self.analyze_revenue_optimization_strategies()
            animal_trends = self.analyze_animal_performance_trends()
            current_performance = self.db.get_current_performance_metrics()
            
            # Calculate overall impact
            total_potential_savings = cost_savings.get('total_potential_savings', 0)
            total_potential_revenue = revenue_opportunities.get('total_potential_increase', 0)
            net_impact = total_potential_revenue - total_potential_savings
            
            # Generate executive summary
            executive_summary = {
                "date": datetime.now().strftime("%Y-%m-%d"),
                "key_findings": {
                    "cost_savings_opportunities": cost_savings.get('total_opportunities', 0),
                    "revenue_opportunities": revenue_opportunities.get('total_opportunities', 0),
                    "total_potential_savings": total_potential_savings,
                    "total_potential_revenue_increase": total_potential_revenue,
                    "net_financial_impact": net_impact
                },
                "priority_actions": self._generate_priority_actions(
                    cost_savings, revenue_opportunities, animal_trends
                ),
                "risk_alerts": self._generate_risk_alerts(animal_trends, cost_breakdown),
                "recommendations": self._generate_executive_recommendations(
                    cost_savings, revenue_opportunities, cost_breakdown, revenue_strategies
                )
            }
            
            return {
                "executive_summary": executive_summary,
                "detailed_analyses": {
                    "cost_savings": cost_savings,
                    "revenue_opportunities": revenue_opportunities,
                    "cost_breakdown": cost_breakdown,
                    "revenue_strategies": revenue_strategies,
                    "animal_trends": animal_trends,
                    "current_performance": current_performance.to_dict('records') if not current_performance.empty else []
                },
                "metadata": {
                    "generated_at": datetime.now().isoformat(),
                    "analysis_period": "30 days",
                    "data_sources": ["gold_animal_costs", "gold_animal_roi", "gold_daily_performance", "silver_visitors", "silver_weather"]
                }
            }
            
        except Exception as e:
            logger.error(f"Error generating comprehensive daily analysis: {e}")
            return {"error": str(e)}
    
    def _generate_cost_recommendations(self, cost_percentages: Dict, cost_drivers: List) -> List[Dict]:
        """Generate cost-specific recommendations."""
        recommendations = []
        
        for driver in cost_drivers:
            if driver['category'] == 'feeding':
                recommendations.append({
                    "category": "feeding",
                    "title": "Optimize Feeding Operations",
                    "description": f"Feeding costs represent {driver['percentage']:.1f}% of total costs.",
                    "actions": [
                        "Review feeding schedules and quantities",
                        "Negotiate better supplier contracts",
                        "Implement waste reduction programs",
                        "Consider bulk purchasing options"
                    ]
                })
            elif driver['category'] == 'medical':
                recommendations.append({
                    "category": "medical",
                    "title": "Optimize Medical Care",
                    "description": f"Medical costs represent {driver['percentage']:.1f}% of total costs.",
                    "actions": [
                        "Review preventive care protocols",
                        "Optimize medication usage",
                        "Consider telemedicine options",
                        "Negotiate veterinary service contracts"
                    ]
                })
            elif driver['category'] == 'maintenance':
                recommendations.append({
                    "category": "maintenance",
                    "title": "Optimize Maintenance Operations",
                    "description": f"Maintenance costs represent {driver['percentage']:.1f}% of total costs.",
                    "actions": [
                        "Implement preventive maintenance schedules",
                        "Review contractor rates and services",
                        "Consider energy-efficient upgrades",
                        "Optimize maintenance frequency"
                    ]
                })
            elif driver['category'] == 'staff':
                recommendations.append({
                    "category": "staff",
                    "title": "Optimize Staff Operations",
                    "description": f"Staff costs represent {driver['percentage']:.1f}% of total costs.",
                    "actions": [
                        "Review staffing levels and schedules",
                        "Implement cross-training programs",
                        "Optimize shift patterns",
                        "Consider automation opportunities"
                    ]
                })
        
        return recommendations
    
    def _generate_priority_actions(self, cost_savings: Dict, revenue_opportunities: Dict, animal_trends: Dict) -> List[Dict]:
        """Generate priority actions based on analysis results."""
        actions = []
        
        # High priority cost savings
        high_cost_opps = [opp for opp in cost_savings.get('opportunities', []) if opp['priority'] == 'high']
        for opp in high_cost_opps[:3]:
            actions.append({
                "type": "cost_savings",
                "priority": "high",
                "title": opp['title'],
                "description": opp['description'],
                "potential_impact": f"${opp['potential_savings']:.2f} daily savings",
                "timeline": "Immediate"
            })
        
        # High priority revenue opportunities
        high_revenue_opps = [opp for opp in revenue_opportunities.get('opportunities', []) if opp['priority'] == 'high']
        for opp in high_revenue_opps[:3]:
            actions.append({
                "type": "revenue_increase",
                "priority": "high",
                "title": opp['title'],
                "description": opp['description'],
                "potential_impact": f"${opp['potential_increase']:.2f} daily increase",
                "timeline": "Immediate"
            })
        
        # Concerning animal trends
        concerning_animals = animal_trends.get('concerning_trends', [])
        for animal in concerning_animals[:2]:
            actions.append({
                "type": "animal_performance",
                "priority": "medium",
                "title": f"Address Declining Performance: {animal['animal_name']}",
                "description": f"{animal['animal_name']} shows declining ROI trend",
                "potential_impact": "Prevent further revenue decline",
                "timeline": "Short term"
            })
        
        return actions
    
    def _generate_risk_alerts(self, animal_trends: Dict, cost_breakdown: Dict) -> List[Dict]:
        """Generate risk alerts based on analysis."""
        alerts = []
        
        # Animal performance risks
        concerning_animals = animal_trends.get('concerning_trends', [])
        if len(concerning_animals) > 5:
            alerts.append({
                "type": "performance_risk",
                "severity": "high",
                "title": "Multiple Animals Showing Declining Performance",
                "description": f"{len(concerning_animals)} animals showing concerning trends",
                "recommendation": "Immediate review of care protocols and visitor experience"
            })
        
        # Cost concentration risks
        cost_drivers = cost_breakdown.get('cost_drivers', [])
        for driver in cost_drivers:
            if driver['percentage'] > 50:
                alerts.append({
                    "type": "cost_concentration_risk",
                    "severity": "medium",
                    "title": f"High Cost Concentration: {driver['category'].title()}",
                    "description": f"{driver['category'].title()} costs represent {driver['percentage']:.1f}% of total",
                    "recommendation": "Diversify cost structure and implement cost controls"
                })
        
        return alerts
    
    def _generate_executive_recommendations(self, cost_savings: Dict, revenue_opportunities: Dict, 
                                          cost_breakdown: Dict, revenue_strategies: Dict) -> List[Dict]:
        """Generate executive-level recommendations."""
        recommendations = []
        
        # Financial impact recommendations
        total_savings = cost_savings.get('total_potential_savings', 0)
        total_revenue = revenue_opportunities.get('total_potential_increase', 0)
        
        if total_savings > 1000:
            recommendations.append({
                "category": "cost_optimization",
                "title": "Implement Cost Savings Initiatives",
                "description": f"Potential daily savings of ${total_savings:.2f} identified",
                "priority": "high",
                "expected_impact": f"${total_savings * 365:.0f} annual savings"
            })
        
        if total_revenue > 1000:
            recommendations.append({
                "category": "revenue_growth",
                "title": "Execute Revenue Optimization Strategies",
                "description": f"Potential daily revenue increase of ${total_revenue:.2f} identified",
                "priority": "high",
                "expected_impact": f"${total_revenue * 365:.0f} annual increase"
            })
        
        # Strategic recommendations
        strategies = revenue_strategies.get('strategies', [])
        high_priority_strategies = [s for s in strategies if s['priority'] == 'high']
        
        for strategy in high_priority_strategies[:2]:
            recommendations.append({
                "category": "strategic_initiative",
                "title": strategy['title'],
                "description": strategy['description'],
                "priority": strategy['priority'],
                "expected_impact": strategy['potential_impact']
            })
        
        return recommendations 