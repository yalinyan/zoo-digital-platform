import streamlit as st
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, date, timedelta
import json
from loguru import logger

from .analyzer import DailyAnalyzer
from .forecaster import RevenueForecaster
from .database import ZooDatabase
from .config import config

class ZooChatbot:
    """AI-powered chatbot for zoo operations and analytics."""
    
    def __init__(self):
        self.analyzer = DailyAnalyzer()
        self.forecaster = RevenueForecaster()
        self.db = ZooDatabase()
        self.conversation_history = []
        
        # Define supported query types
        self.query_types = {
            'cost_analysis': ['cost', 'savings', 'expense', 'budget', 'spending'],
            'revenue_analysis': ['revenue', 'income', 'profit', 'earnings', 'sales'],
            'performance': ['performance', 'metrics', 'kpi', 'roi', 'efficiency'],
            'forecasting': ['forecast', 'prediction', 'trend', 'future', 'projection'],
            'animal_info': ['animal', 'species', 'enclosure', 'care', 'feeding'],
            'visitor_analysis': ['visitor', 'guest', 'attendance', 'satisfaction'],
            'weather_impact': ['weather', 'temperature', 'climate', 'seasonal'],
            'general': ['help', 'hello', 'hi', 'what', 'how', 'when', 'where']
        }
    
    def classify_query(self, user_input: str) -> str:
        """Classify user query into categories."""
        user_input_lower = user_input.lower()
        
        for query_type, keywords in self.query_types.items():
            if any(keyword in user_input_lower for keyword in keywords):
                return query_type
        
        return 'general'
    
    def get_cost_analysis_response(self, query: str) -> str:
        """Generate response for cost-related queries."""
        try:
            analysis = self.analyzer.analyze_cost_savings()
            
            if 'opportunities' in query.lower() or 'savings' in query.lower():
                opportunities = analysis.get('opportunities', [])
                if opportunities:
                    response = f"💰 **Cost Savings Opportunities**\n\n"
                    response += f"Found {len(opportunities)} opportunities with potential savings of ${analysis.get('total_potential_savings', 0):.2f} daily.\n\n"
                    
                    for i, opp in enumerate(opportunities[:3], 1):
                        response += f"**{i}. {opp['title']}**\n"
                        response += f"   {opp['description']}\n"
                        response += f"   Potential savings: ${opp['potential_savings']:.2f}/day\n"
                        response += f"   Priority: {opp['priority'].title()}\n\n"
                    
                    if len(opportunities) > 3:
                        response += f"... and {len(opportunities) - 3} more opportunities.\n"
                else:
                    response = "✅ No significant cost savings opportunities identified at this time."
            
            elif 'high cost' in query.lower() or 'expensive' in query.lower():
                high_cost_animals = self.db.get_high_cost_animals()
                if not high_cost_animals.empty:
                    response = "🐘 **High Cost Animals**\n\n"
                    for _, animal in high_cost_animals.head(5).iterrows():
                        response += f"• **{animal['animal_name']}** ({animal['species']}): ${animal['avg_daily_cost']:.2f}/day\n"
                else:
                    response = "No high-cost animals identified in the current data."
            
            else:
                response = f"📊 **Cost Analysis Summary**\n\n"
                response += f"• Total opportunities: {analysis.get('total_opportunities', 0)}\n"
                response += f"• Potential daily savings: ${analysis.get('total_potential_savings', 0):.2f}\n"
                response += f"• High priority items: {analysis.get('summary', {}).get('high_priority', 0)}\n"
            
            return response
            
        except Exception as e:
            logger.error(f"Error in cost analysis response: {e}")
            return "Sorry, I encountered an error while analyzing cost data. Please try again."
    
    def get_revenue_analysis_response(self, query: str) -> str:
        """Generate response for revenue-related queries."""
        try:
            analysis = self.analyzer.analyze_revenue_opportunities()
            
            if 'opportunities' in query.lower() or 'increase' in query.lower():
                opportunities = analysis.get('opportunities', [])
                if opportunities:
                    response = f"💵 **Revenue Opportunities**\n\n"
                    response += f"Found {len(opportunities)} opportunities with potential increase of ${analysis.get('total_potential_increase', 0):.2f} daily.\n\n"
                    
                    for i, opp in enumerate(opportunities[:3], 1):
                        response += f"**{i}. {opp['title']}**\n"
                        response += f"   {opp['description']}\n"
                        response += f"   Potential increase: ${opp['potential_increase']:.2f}/day\n"
                        response += f"   Priority: {opp['priority'].title()}\n\n"
                    
                    if len(opportunities) > 3:
                        response += f"... and {len(opportunities) - 3} more opportunities.\n"
                else:
                    response = "✅ No significant revenue opportunities identified at this time."
            
            elif 'low performing' in query.lower() or 'underperforming' in query.lower():
                low_performing = self.db.get_low_performing_animals()
                if not low_performing.empty:
                    response = "📉 **Low Performing Animals**\n\n"
                    for _, animal in low_performing.head(5).iterrows():
                        response += f"• **{animal['animal_name']}**: ${animal['avg_daily_revenue']:.2f}/day revenue\n"
                else:
                    response = "No low-performing animals identified in the current data."
            
            else:
                response = f"📈 **Revenue Analysis Summary**\n\n"
                response += f"• Total opportunities: {analysis.get('total_opportunities', 0)}\n"
                response += f"• Potential daily increase: ${analysis.get('total_potential_increase', 0):.2f}\n"
                response += f"• High priority items: {analysis.get('summary', {}).get('high_priority', 0)}\n"
            
            return response
            
        except Exception as e:
            logger.error(f"Error in revenue analysis response: {e}")
            return "Sorry, I encountered an error while analyzing revenue data. Please try again."
    
    def get_performance_response(self, query: str) -> str:
        """Generate response for performance-related queries."""
        try:
            metrics = self.db.get_current_performance_metrics()
            
            if not metrics.empty:
                latest = metrics.iloc[-1]
                response = "📊 **Current Performance Metrics**\n\n"
                response += f"• **Revenue**: ${latest.get('total_revenue_usd', 0):.2f}\n"
                response += f"• **Costs**: ${latest.get('total_cost_usd', 0):.2f}\n"
                response += f"• **Profit**: ${latest.get('total_profit_usd', 0):.2f}\n"
                response += f"• **Profit Margin**: {latest.get('profit_margin_percentage', 0):.1f}%\n"
                response += f"• **Visitors**: {latest.get('total_visitors', 0)}\n"
                response += f"• **Revenue per Visitor**: ${latest.get('revenue_per_visitor_usd', 0):.2f}\n"
                response += f"• **Satisfaction**: {latest.get('avg_visitor_satisfaction', 0):.1f}/5\n"
            else:
                response = "No recent performance data available."
            
            return response
            
        except Exception as e:
            logger.error(f"Error in performance response: {e}")
            return "Sorry, I encountered an error while retrieving performance data. Please try again."
    
    def get_forecasting_response(self, query: str) -> str:
        """Generate response for forecasting queries."""
        try:
            if 'revenue' in query.lower():
                forecast = self.forecaster.forecast_revenue(days=30)
                if forecast and 'forecast' in forecast:
                    response = "🔮 **Revenue Forecast (30 days)**\n\n"
                    forecast_data = forecast['forecast']
                    
                    response += f"• **Predicted Revenue**: ${forecast_data.get('predicted_revenue', 0):.2f}\n"
                    response += f"• **Confidence Interval**: ±${forecast_data.get('confidence_interval', 0):.2f}\n"
                    response += f"• **Trend**: {forecast_data.get('trend', 'Stable')}\n"
                    
                    if 'seasonal_factors' in forecast_data:
                        response += f"• **Seasonal Impact**: {forecast_data['seasonal_factors']}\n"
                else:
                    response = "Unable to generate revenue forecast at this time."
            
            elif 'visitors' in query.lower():
                forecast = self.forecaster.forecast_visitors(days=30)
                if forecast and 'forecast' in forecast:
                    response = "👥 **Visitor Forecast (30 days)**\n\n"
                    forecast_data = forecast['forecast']
                    
                    response += f"• **Predicted Visitors**: {forecast_data.get('predicted_visitors', 0):.0f}\n"
                    response += f"• **Confidence Interval**: ±{forecast_data.get('confidence_interval', 0):.0f}\n"
                    response += f"• **Peak Days**: {forecast_data.get('peak_days', 'Weekends')}\n"
                else:
                    response = "Unable to generate visitor forecast at this time."
            
            else:
                response = "I can help you with revenue and visitor forecasts. Try asking about 'revenue forecast' or 'visitor forecast'."
            
            return response
            
        except Exception as e:
            logger.error(f"Error in forecasting response: {e}")
            return "Sorry, I encountered an error while generating forecasts. Please try again."
    
    def get_animal_info_response(self, query: str) -> str:
        """Generate response for animal-related queries."""
        try:
            # Extract animal name or species from query
            query_lower = query.lower()
            
            if 'species' in query_lower:
                species_data = self.db.get_species_roi_data(days=30)
                if not species_data.empty:
                    response = "🦁 **Species Performance**\n\n"
                    for _, species in species_data.head(5).iterrows():
                        response += f"• **{species['species']}**: ROI {species['avg_daily_roi_percentage']:.1f}%\n"
                else:
                    response = "No species data available."
            
            elif 'feeding' in query_lower:
                feeding_data = self.db.get_feeding_data(days=7)
                if not feeding_data.empty:
                    response = "🍎 **Recent Feeding Data**\n\n"
                    response += f"• Total feedings: {len(feeding_data)}\n"
                    response += f"• Average consumption: {feeding_data['consumption_percentage'].mean():.1f}%\n"
                    response += f"• Total cost: ${feeding_data['cost_usd'].sum():.2f}\n"
                else:
                    response = "No recent feeding data available."
            
            else:
                response = "I can help you with animal information. Try asking about 'species performance', 'feeding data', or specific animals."
            
            return response
            
        except Exception as e:
            logger.error(f"Error in animal info response: {e}")
            return "Sorry, I encountered an error while retrieving animal data. Please try again."
    
    def get_visitor_analysis_response(self, query: str) -> str:
        """Generate response for visitor-related queries."""
        try:
            visitor_data = self.db.get_visitor_data(days=30)
            
            if not visitor_data.empty:
                response = "👥 **Visitor Analysis**\n\n"
                response += f"• **Total Visitors**: {len(visitor_data)}\n"
                response += f"• **Average Satisfaction**: {visitor_data['satisfaction_rating'].mean():.1f}/5\n"
                response += f"• **Average Spend**: ${visitor_data['total_spent_usd'].mean():.2f}\n"
                response += f"• **Average Visit Duration**: {visitor_data['visit_duration_minutes'].mean():.0f} minutes\n"
                
                # Ticket type breakdown
                ticket_types = visitor_data['ticket_type'].value_counts()
                response += f"\n**Ticket Types**:\n"
                for ticket_type, count in ticket_types.head(3).items():
                    response += f"• {ticket_type}: {count} visitors\n"
            else:
                response = "No visitor data available for the specified period."
            
            return response
            
        except Exception as e:
            logger.error(f"Error in visitor analysis response: {e}")
            return "Sorry, I encountered an error while analyzing visitor data. Please try again."
    
    def get_weather_impact_response(self, query: str) -> str:
        """Generate response for weather-related queries."""
        try:
            weather_data = self.db.get_weather_data(days=30)
            performance_data = self.db.get_performance_metrics(days=30)
            
            if not weather_data.empty and not performance_data.empty:
                response = "🌤️ **Weather Impact Analysis**\n\n"
                
                # Merge weather and performance data
                merged_data = pd.merge(weather_data, performance_data, on='date', how='inner')
                
                if not merged_data.empty:
                    # Weather condition impact
                    weather_impact = merged_data.groupby('condition').agg({
                        'total_revenue_usd': 'mean',
                        'total_visitors': 'mean'
                    }).round(2)
                    
                    response += "**Revenue by Weather Condition**:\n"
                    for condition, data in weather_impact.iterrows():
                        response += f"• {condition}: ${data['total_revenue_usd']:.2f} avg revenue\n"
                    
                    response += f"\n**Best Weather**: {weather_impact['total_revenue_usd'].idxmax()}\n"
                    response += f"**Worst Weather**: {weather_impact['total_revenue_usd'].idxmin()}\n"
                else:
                    response = "Insufficient data to analyze weather impact."
            else:
                response = "No weather or performance data available."
            
            return response
            
        except Exception as e:
            logger.error(f"Error in weather impact response: {e}")
            return "Sorry, I encountered an error while analyzing weather impact. Please try again."
    
    def get_general_response(self, query: str) -> str:
        """Generate response for general queries."""
        query_lower = query.lower()
        
        if any(word in query_lower for word in ['help', 'what can you do', 'capabilities']):
            response = """🤖 **Zoo AI Assistant - What I Can Help With:**

**📊 Analytics & Insights:**
• Cost savings opportunities and analysis
• Revenue optimization strategies
• Performance metrics and KPIs
• ROI analysis for animals and species

**🔮 Forecasting:**
• Revenue predictions
• Visitor attendance forecasts
• Seasonal trend analysis

**🐾 Animal Information:**
• Species performance data
• Feeding patterns and costs
• Animal care metrics

**👥 Visitor Analysis:**
• Visitor satisfaction trends
• Attendance patterns
• Spending behavior

**🌤️ Weather Impact:**
• Weather-revenue correlations
• Seasonal performance analysis

**💡 Examples:**
• "Show me cost savings opportunities"
• "What's the revenue forecast for next month?"
• "Which animals are underperforming?"
• "How does weather affect our revenue?"

Just ask me anything about zoo operations and analytics!"""
        
        elif any(word in query_lower for word in ['hello', 'hi', 'hey']):
            response = "👋 Hello! I'm your Zoo AI Assistant. I can help you with cost analysis, revenue optimization, forecasting, and more. What would you like to know about today?"
        
        elif 'daily report' in query_lower or 'summary' in query_lower:
            try:
                report = self.analyzer.generate_daily_report()
                response = "📋 **Daily Analysis Report**\n\n"
                response += f"**Cost Savings**: ${report.get('summary', {}).get('total_potential_savings', 0):.2f} potential daily savings\n"
                response += f"**Revenue Opportunities**: ${report.get('summary', {}).get('total_potential_revenue_increase', 0):.2f} potential daily increase\n"
                response += f"**Net Impact**: ${report.get('summary', {}).get('net_potential_impact', 0):.2f} net potential daily impact\n\n"
                
                high_priority = report.get('high_priority_recommendations', [])
                if high_priority:
                    response += "**Top Recommendations**:\n"
                    for i, rec in enumerate(high_priority[:3], 1):
                        response += f"{i}. {rec['title']}\n"
            except Exception as e:
                response = "Sorry, I couldn't generate the daily report at this time."
        
        else:
            response = "I'm not sure I understand. Try asking me about cost analysis, revenue opportunities, forecasting, or type 'help' to see what I can do!"
        
        return response
    
    def process_query(self, user_input: str) -> str:
        """Process user query and generate appropriate response."""
        try:
            # Add to conversation history
            self.conversation_history.append({
                'timestamp': datetime.now().isoformat(),
                'user_input': user_input,
                'query_type': 'unknown'
            })
            
            # Classify query
            query_type = self.classify_query(user_input)
            
            # Update conversation history
            self.conversation_history[-1]['query_type'] = query_type
            
            # Generate response based on query type
            if query_type == 'cost_analysis':
                response = self.get_cost_analysis_response(user_input)
            elif query_type == 'revenue_analysis':
                response = self.get_revenue_analysis_response(user_input)
            elif query_type == 'performance':
                response = self.get_performance_response(user_input)
            elif query_type == 'forecasting':
                response = self.get_forecasting_response(user_input)
            elif query_type == 'animal_info':
                response = self.get_animal_info_response(user_input)
            elif query_type == 'visitor_analysis':
                response = self.get_visitor_analysis_response(user_input)
            elif query_type == 'weather_impact':
                response = self.get_weather_impact_response(user_input)
            else:
                response = self.get_general_response(user_input)
            
            return response
            
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            return "I'm sorry, I encountered an error while processing your request. Please try again or ask for help."

def create_streamlit_chatbot():
    """Create Streamlit chatbot interface."""
    st.title("🤖 Zoo AI Assistant")
    st.markdown("Ask me anything about zoo operations, analytics, and insights!")
    
    # Initialize chatbot
    if 'chatbot' not in st.session_state:
        st.session_state.chatbot = ZooChatbot()
    
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    
    # Chat interface
    user_input = st.chat_input("Ask me about cost savings, revenue opportunities, forecasting, or anything else...")
    
    if user_input:
        # Add user message to chat
        st.session_state.chat_history.append({"role": "user", "content": user_input})
        
        # Get bot response
        with st.spinner("Analyzing..."):
            bot_response = st.session_state.chatbot.process_query(user_input)
        
        # Add bot response to chat
        st.session_state.chat_history.append({"role": "assistant", "content": bot_response})
    
    # Display chat history
    for message in st.session_state.chat_history:
        if message["role"] == "user":
            st.chat_message("user").write(message["content"])
        else:
            st.chat_message("assistant").markdown(message["content"])
    
    # Sidebar with quick actions
    with st.sidebar:
        st.header("🚀 Quick Actions")
        
        if st.button("📊 Daily Report"):
            with st.spinner("Generating daily report..."):
                report = st.session_state.chatbot.analyzer.generate_daily_report()
                st.session_state.chat_history.append({
                    "role": "user", 
                    "content": "Generate daily report"
                })
                st.session_state.chat_history.append({
                    "role": "assistant", 
                    "content": f"📋 **Daily Analysis Report**\n\n**Cost Savings**: ${report.get('summary', {}).get('total_potential_savings', 0):.2f} potential daily savings\n**Revenue Opportunities**: ${report.get('summary', {}).get('total_potential_revenue_increase', 0):.2f} potential daily increase\n**Net Impact**: ${report.get('summary', {}).get('net_potential_impact', 0):.2f} net potential daily impact"
                })
        
        if st.button("💰 Cost Analysis"):
            st.session_state.chat_history.append({
                "role": "user", 
                "content": "Show me cost savings opportunities"
            })
            st.session_state.chat_history.append({
                "role": "assistant", 
                "content": st.session_state.chatbot.get_cost_analysis_response("cost savings opportunities")
            })
        
        if st.button("💵 Revenue Analysis"):
            st.session_state.chat_history.append({
                "role": "user", 
                "content": "Show me revenue opportunities"
            })
            st.session_state.chat_history.append({
                "role": "assistant", 
                "content": st.session_state.chatbot.get_revenue_analysis_response("revenue opportunities")
            })
        
        if st.button("🔮 Revenue Forecast"):
            st.session_state.chat_history.append({
                "role": "user", 
                "content": "What's the revenue forecast for next month?"
            })
            st.session_state.chat_history.append({
                "role": "assistant", 
                "content": st.session_state.chatbot.get_forecasting_response("revenue forecast")
            })
        
        if st.button("📈 Performance Metrics"):
            st.session_state.chat_history.append({
                "role": "user", 
                "content": "Show me current performance metrics"
            })
            st.session_state.chat_history.append({
                "role": "assistant", 
                "content": st.session_state.chatbot.get_performance_response("performance metrics")
            })
        
        # Clear chat button
        if st.button("🗑️ Clear Chat"):
            st.session_state.chat_history = []
            st.rerun()

if __name__ == "__main__":
    create_streamlit_chatbot() 