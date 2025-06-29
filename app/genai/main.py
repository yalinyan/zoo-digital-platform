import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json

from .chatbot import ZooChatbot, create_streamlit_chatbot
from .analyzer import DailyAnalyzer
from .forecaster import RevenueForecaster
from .database import ZooDatabase

def create_dashboard():
    """Create the main zoo analytics dashboard."""
    st.set_page_config(
        page_title="Zoo Digital Platform",
        page_icon="🦁",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Initialize components
    if 'chatbot' not in st.session_state:
        st.session_state.chatbot = ZooChatbot()
    
    if 'analyzer' not in st.session_state:
        st.session_state.analyzer = DailyAnalyzer()
    
    if 'forecaster' not in st.session_state:
        st.session_state.forecaster = RevenueForecaster()
    
    if 'db' not in st.session_state:
        st.session_state.db = ZooDatabase()
    
    # Sidebar navigation
    st.sidebar.title("🦁 Zoo Digital Platform")
    page = st.sidebar.selectbox(
        "Navigation",
        ["Dashboard", "AI Assistant", "Daily Analysis", "Cost Analysis", "Revenue Analysis", "Forecasting"]
    )
    
    if page == "Dashboard":
        show_dashboard()
    elif page == "AI Assistant":
        show_ai_assistant()
    elif page == "Daily Analysis":
        show_daily_analysis()
    elif page == "Cost Analysis":
        show_cost_analysis()
    elif page == "Revenue Analysis":
        show_revenue_analysis()
    elif page == "Forecasting":
        show_forecasting()

def show_dashboard():
    """Show the main dashboard."""
    st.title("🦁 Zoo Digital Platform Dashboard")
    st.markdown("Welcome to your comprehensive zoo analytics and AI assistant platform!")
    
    # Key metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    try:
        # Get current performance metrics
        performance = st.session_state.db.get_current_performance_metrics()
        
        if not performance.empty:
            latest = performance.iloc[-1]
            
            with col1:
                st.metric(
                    "Daily Revenue",
                    f"${latest.get('total_revenue_usd', 0):,.0f}",
                    delta=f"${latest.get('total_revenue_usd', 0) * 0.05:.0f}"  # Placeholder delta
                )
            
            with col2:
                st.metric(
                    "Daily Profit",
                    f"${latest.get('total_profit_usd', 0):,.0f}",
                    delta=f"${latest.get('total_profit_usd', 0) * 0.03:.0f}"  # Placeholder delta
                )
            
            with col3:
                st.metric(
                    "Daily Visitors",
                    f"{latest.get('total_visitors', 0):,}",
                    delta=f"{latest.get('total_visitors', 0) * 0.02:.0f}"  # Placeholder delta
                )
            
            with col4:
                st.metric(
                    "Satisfaction",
                    f"{latest.get('avg_visitor_satisfaction', 0):.1f}/5",
                    delta="0.1"  # Placeholder delta
                )
        else:
            # Placeholder metrics if no data
            with col1:
                st.metric("Daily Revenue", "$15,000", delta="$750")
            with col2:
                st.metric("Daily Profit", "$5,000", delta="$250")
            with col3:
                st.metric("Daily Visitors", "1,200", delta="60")
            with col4:
                st.metric("Satisfaction", "4.2/5", delta="0.1")
    
    except Exception as e:
        st.error(f"Error loading metrics: {e}")
        # Fallback metrics
        with col1:
            st.metric("Daily Revenue", "$15,000", delta="$750")
        with col2:
            st.metric("Daily Profit", "$5,000", delta="$250")
        with col3:
            st.metric("Daily Visitors", "1,200", delta="60")
        with col4:
            st.metric("Satisfaction", "4.2/5", delta="0.1")
    
    # Quick actions
    st.subheader("🚀 Quick Actions")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📊 Generate Daily Report", use_container_width=True):
            with st.spinner("Generating comprehensive daily report..."):
                try:
                    report = st.session_state.analyzer.generate_comprehensive_daily_analysis()
                    st.session_state.daily_report = report
                    st.success("Daily report generated successfully!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error generating report: {e}")
    
    with col2:
        if st.button("💰 Cost Analysis", use_container_width=True):
            st.session_state.show_cost_analysis = True
            st.rerun()
    
    with col3:
        if st.button("💵 Revenue Analysis", use_container_width=True):
            st.session_state.show_revenue_analysis = True
            st.rerun()
    
    # Recent insights
    st.subheader("📈 Recent Insights")
    
    try:
        # Get recent performance data
        performance_data = st.session_state.db.get_performance_metrics(days=7)
        
        if not performance_data.empty:
            # Revenue trend chart
            fig_revenue = px.line(
                performance_data, 
                x='date', 
                y='total_revenue_usd',
                title="Daily Revenue Trend (Last 7 Days)",
                labels={'total_revenue_usd': 'Revenue ($)', 'date': 'Date'}
            )
            st.plotly_chart(fig_revenue, use_container_width=True)
            
            # Visitor trend chart
            col1, col2 = st.columns(2)
            with col1:
                fig_visitors = px.line(
                    performance_data, 
                    x='date', 
                    y='total_visitors',
                    title="Daily Visitors",
                    labels={'total_visitors': 'Visitors', 'date': 'Date'}
                )
                st.plotly_chart(fig_visitors, use_container_width=True)
            
            with col2:
                fig_satisfaction = px.line(
                    performance_data, 
                    x='date', 
                    y='avg_visitor_satisfaction',
                    title="Visitor Satisfaction",
                    labels={'avg_visitor_satisfaction': 'Satisfaction (1-5)', 'date': 'Date'}
                )
                st.plotly_chart(fig_satisfaction, use_container_width=True)
        else:
            st.info("No recent performance data available. Run the data ingestion to populate the dashboard.")
    
    except Exception as e:
        st.error(f"Error loading charts: {e}")

def show_ai_assistant():
    """Show the AI assistant interface."""
    st.title("🤖 Zoo AI Assistant")
    st.markdown("Ask me anything about zoo operations, analytics, and insights!")
    
    # Initialize chat history
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
    
    # Quick action buttons
    st.sidebar.header("🚀 Quick Actions")
    
    if st.sidebar.button("📊 Daily Report"):
        with st.spinner("Generating daily report..."):
            try:
                report = st.session_state.analyzer.generate_daily_report()
                st.session_state.chat_history.append({
                    "role": "user", 
                    "content": "Generate daily report"
                })
                st.session_state.chat_history.append({
                    "role": "assistant", 
                    "content": f"📋 **Daily Analysis Report**\n\n**Cost Savings**: ${report.get('summary', {}).get('total_potential_savings', 0):.2f} potential daily savings\n**Revenue Opportunities**: ${report.get('summary', {}).get('total_potential_revenue_increase', 0):.2f} potential daily increase\n**Net Impact**: ${report.get('summary', {}).get('net_potential_impact', 0):.2f} net potential daily impact"
                })
                st.rerun()
            except Exception as e:
                st.error(f"Error generating report: {e}")
    
    if st.sidebar.button("💰 Cost Analysis"):
        st.session_state.chat_history.append({
            "role": "user", 
            "content": "Show me cost savings opportunities"
        })
        st.session_state.chat_history.append({
            "role": "assistant", 
            "content": st.session_state.chatbot.get_cost_analysis_response("cost savings opportunities")
        })
        st.rerun()
    
    if st.sidebar.button("💵 Revenue Analysis"):
        st.session_state.chat_history.append({
            "role": "user", 
            "content": "Show me revenue opportunities"
        })
        st.session_state.chat_history.append({
            "role": "assistant", 
            "content": st.session_state.chatbot.get_revenue_analysis_response("revenue opportunities")
        })
        st.rerun()
    
    if st.sidebar.button("🔮 Revenue Forecast"):
        st.session_state.chat_history.append({
            "role": "user", 
            "content": "What's the revenue forecast for next month?"
        })
        st.session_state.chat_history.append({
            "role": "assistant", 
            "content": st.session_state.chatbot.get_forecasting_response("revenue forecast")
        })
        st.rerun()
    
    if st.sidebar.button("📈 Performance Metrics"):
        st.session_state.chat_history.append({
            "role": "user", 
            "content": "Show me current performance metrics"
        })
        st.session_state.chat_history.append({
            "role": "assistant", 
            "content": st.session_state.chatbot.get_performance_response("performance metrics")
        })
        st.rerun()
    
    # Clear chat button
    if st.sidebar.button("🗑️ Clear Chat"):
        st.session_state.chat_history = []
        st.rerun()

def show_daily_analysis():
    """Show comprehensive daily analysis."""
    st.title("📊 Daily Analysis Report")
    
    if st.button("🔄 Generate New Analysis"):
        with st.spinner("Generating comprehensive daily analysis..."):
            try:
                analysis = st.session_state.analyzer.generate_comprehensive_daily_analysis()
                st.session_state.comprehensive_analysis = analysis
                st.success("Analysis completed successfully!")
                st.rerun()
            except Exception as e:
                st.error(f"Error generating analysis: {e}")
    
    # Display analysis if available
    if hasattr(st.session_state, 'comprehensive_analysis') and st.session_state.comprehensive_analysis:
        analysis = st.session_state.comprehensive_analysis
        
        # Executive Summary
        st.subheader("📋 Executive Summary")
        summary = analysis.get('executive_summary', {})
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Cost Opportunities", summary.get('key_findings', {}).get('cost_savings_opportunities', 0))
        with col2:
            st.metric("Revenue Opportunities", summary.get('key_findings', {}).get('revenue_opportunities', 0))
        with col3:
            st.metric("Potential Savings", f"${summary.get('key_findings', {}).get('total_potential_savings', 0):,.0f}")
        with col4:
            st.metric("Potential Revenue", f"${summary.get('key_findings', {}).get('total_potential_revenue_increase', 0):,.0f}")
        
        # Priority Actions
        st.subheader("🎯 Priority Actions")
        priority_actions = summary.get('priority_actions', [])
        
        for i, action in enumerate(priority_actions[:5], 1):
            with st.expander(f"{i}. {action['title']}"):
                st.write(f"**Description**: {action['description']}")
                st.write(f"**Potential Impact**: {action['potential_impact']}")
                st.write(f"**Timeline**: {action['timeline']}")
                st.write(f"**Priority**: {action['priority'].title()}")
        
        # Risk Alerts
        st.subheader("⚠️ Risk Alerts")
        risk_alerts = summary.get('risk_alerts', [])
        
        if risk_alerts:
            for alert in risk_alerts:
                severity_color = "🔴" if alert['severity'] == 'high' else "🟡"
                st.warning(f"{severity_color} **{alert['title']}**\n{alert['description']}\n**Recommendation**: {alert['recommendation']}")
        else:
            st.success("✅ No significant risks identified")
        
        # Detailed Analyses
        st.subheader("📈 Detailed Analyses")
        
        detailed = analysis.get('detailed_analyses', {})
        
        # Cost Analysis
        if 'cost_savings' in detailed:
            with st.expander("💰 Cost Savings Analysis"):
                cost_data = detailed['cost_savings']
                if 'opportunities' in cost_data:
                    for opp in cost_data['opportunities'][:3]:
                        st.write(f"**{opp['title']}**")
                        st.write(f"{opp['description']}")
                        st.write(f"Potential savings: ${opp['potential_savings']:.2f}/day")
                        st.write("---")
        
        # Revenue Analysis
        if 'revenue_opportunities' in detailed:
            with st.expander("💵 Revenue Opportunities"):
                revenue_data = detailed['revenue_opportunities']
                if 'opportunities' in revenue_data:
                    for opp in revenue_data['opportunities'][:3]:
                        st.write(f"**{opp['title']}**")
                        st.write(f"{opp['description']}")
                        st.write(f"Potential increase: ${opp['potential_increase']:.2f}/day")
                        st.write("---")
        
        # Animal Trends
        if 'animal_trends' in detailed:
            with st.expander("🦁 Animal Performance Trends"):
                trends_data = detailed['animal_trends']
                if 'concerning_trends' in trends_data:
                    st.write(f"**Animals with concerning trends**: {len(trends_data['concerning_trends'])}")
                    for animal in trends_data['concerning_trends'][:3]:
                        st.write(f"• {animal['animal_name']} ({animal['species']}): {animal['roi_trend']} ROI trend")
    else:
        st.info("Click 'Generate New Analysis' to create a comprehensive daily analysis report.")

def show_cost_analysis():
    """Show detailed cost analysis."""
    st.title("💰 Cost Analysis")
    
    if st.button("🔄 Refresh Cost Analysis"):
        with st.spinner("Analyzing costs..."):
            try:
                cost_analysis = st.session_state.analyzer.analyze_cost_savings()
                cost_breakdown = st.session_state.analyzer.analyze_detailed_cost_breakdown()
                st.session_state.cost_analysis = cost_analysis
                st.session_state.cost_breakdown = cost_breakdown
                st.success("Cost analysis completed!")
                st.rerun()
            except Exception as e:
                st.error(f"Error analyzing costs: {e}")
    
    # Display cost analysis if available
    if hasattr(st.session_state, 'cost_analysis') and st.session_state.cost_analysis:
        cost_data = st.session_state.cost_analysis
        
        # Summary metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Opportunities", cost_data.get('total_opportunities', 0))
        with col2:
            st.metric("Potential Daily Savings", f"${cost_data.get('total_potential_savings', 0):,.2f}")
        with col3:
            st.metric("High Priority Items", cost_data.get('summary', {}).get('high_priority', 0))
        
        # Cost breakdown chart
        if hasattr(st.session_state, 'cost_breakdown') and st.session_state.cost_breakdown:
            breakdown = st.session_state.cost_breakdown
            
            if 'cost_breakdown' in breakdown:
                cost_categories = list(breakdown['cost_breakdown'].keys())
                cost_values = list(breakdown['cost_breakdown'].values())
                
                fig = px.pie(
                    values=cost_values,
                    names=cost_categories,
                    title="Cost Breakdown by Category"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Opportunities
        st.subheader("🎯 Cost Savings Opportunities")
        opportunities = cost_data.get('opportunities', [])
        
        for i, opp in enumerate(opportunities, 1):
            with st.expander(f"{i}. {opp['title']}"):
                st.write(f"**Description**: {opp['description']}")
                st.write(f"**Potential Savings**: ${opp['potential_savings']:.2f}/day")
                st.write(f"**Priority**: {opp['priority'].title()}")
                st.write("**Recommendations**:")
                for rec in opp['recommendations']:
                    st.write(f"• {rec}")
    else:
        st.info("Click 'Refresh Cost Analysis' to analyze cost savings opportunities.")

def show_revenue_analysis():
    """Show detailed revenue analysis."""
    st.title("💵 Revenue Analysis")
    
    if st.button("🔄 Refresh Revenue Analysis"):
        with st.spinner("Analyzing revenue opportunities..."):
            try:
                revenue_analysis = st.session_state.analyzer.analyze_revenue_opportunities()
                revenue_strategies = st.session_state.analyzer.analyze_revenue_optimization_strategies()
                st.session_state.revenue_analysis = revenue_analysis
                st.session_state.revenue_strategies = revenue_strategies
                st.success("Revenue analysis completed!")
                st.rerun()
            except Exception as e:
                st.error(f"Error analyzing revenue: {e}")
    
    # Display revenue analysis if available
    if hasattr(st.session_state, 'revenue_analysis') and st.session_state.revenue_analysis:
        revenue_data = st.session_state.revenue_analysis
        
        # Summary metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Opportunities", revenue_data.get('total_opportunities', 0))
        with col2:
            st.metric("Potential Daily Increase", f"${revenue_data.get('total_potential_increase', 0):,.2f}")
        with col3:
            st.metric("High Priority Items", revenue_data.get('summary', {}).get('high_priority', 0))
        
        # Revenue strategies
        if hasattr(st.session_state, 'revenue_strategies') and st.session_state.revenue_strategies:
            strategies = st.session_state.revenue_strategies.get('strategies', [])
            
            st.subheader("📈 Revenue Optimization Strategies")
            for i, strategy in enumerate(strategies, 1):
                with st.expander(f"{i}. {strategy['title']}"):
                    st.write(f"**Description**: {strategy['description']}")
                    st.write(f"**Potential Impact**: {strategy['potential_impact']}")
                    st.write(f"**Implementation**: {strategy['implementation']}")
                    st.write(f"**Priority**: {strategy['priority'].title()}")
        
        # Opportunities
        st.subheader("🎯 Revenue Opportunities")
        opportunities = revenue_data.get('opportunities', [])
        
        for i, opp in enumerate(opportunities, 1):
            with st.expander(f"{i}. {opp['title']}"):
                st.write(f"**Description**: {opp['description']}")
                st.write(f"**Potential Increase**: ${opp['potential_increase']:.2f}/day")
                st.write(f"**Priority**: {opp['priority'].title()}")
                st.write("**Recommendations**:")
                for rec in opp['recommendations']:
                    st.write(f"• {rec}")
    else:
        st.info("Click 'Refresh Revenue Analysis' to analyze revenue opportunities.")

def show_forecasting():
    """Show forecasting analysis."""
    st.title("🔮 Forecasting & Predictions")
    
    # Forecast options
    forecast_type = st.selectbox(
        "Select Forecast Type",
        ["Revenue Forecast", "Visitor Forecast", "Cost Forecast"]
    )
    
    forecast_days = st.slider("Forecast Period (days)", 7, 90, 30)
    
    if st.button("🔮 Generate Forecast"):
        with st.spinner(f"Generating {forecast_type.lower()}..."):
            try:
                if forecast_type == "Revenue Forecast":
                    forecast = st.session_state.forecaster.forecast_revenue(days=forecast_days)
                elif forecast_type == "Visitor Forecast":
                    forecast = st.session_state.forecaster.forecast_visitors(days=forecast_days)
                else:
                    forecast = st.session_state.forecaster.forecast_costs(days=forecast_days)
                
                st.session_state.current_forecast = forecast
                st.success("Forecast generated successfully!")
                st.rerun()
            except Exception as e:
                st.error(f"Error generating forecast: {e}")
    
    # Display forecast if available
    if hasattr(st.session_state, 'current_forecast') and st.session_state.current_forecast:
        forecast = st.session_state.current_forecast
        
        if 'forecast' in forecast:
            forecast_data = forecast['forecast']
            
            st.subheader(f"📊 {forecast_type} Results")
            
            col1, col2, col3 = st.columns(3)
            
            if forecast_type == "Revenue Forecast":
                with col1:
                    st.metric("Predicted Revenue", f"${forecast_data.get('predicted_revenue', 0):,.2f}")
                with col2:
                    st.metric("Confidence Interval", f"±${forecast_data.get('confidence_interval', 0):,.2f}")
                with col3:
                    st.metric("Trend", forecast_data.get('trend', 'Stable'))
            
            elif forecast_type == "Visitor Forecast":
                with col1:
                    st.metric("Predicted Visitors", f"{forecast_data.get('predicted_visitors', 0):,.0f}")
                with col2:
                    st.metric("Confidence Interval", f"±{forecast_data.get('confidence_interval', 0):,.0f}")
                with col3:
                    st.metric("Peak Days", forecast_data.get('peak_days', 'Weekends'))
            
            else:  # Cost Forecast
                with col1:
                    st.metric("Predicted Costs", f"${forecast_data.get('predicted_costs', 0):,.2f}")
                with col2:
                    st.metric("Confidence Interval", f"±${forecast_data.get('confidence_interval', 0):,.2f}")
                with col3:
                    st.metric("Trend", forecast_data.get('trend', 'Stable'))
            
            # Display forecast details
            if 'seasonal_factors' in forecast_data:
                st.write(f"**Seasonal Impact**: {forecast_data['seasonal_factors']}")
            
            if 'key_factors' in forecast_data:
                st.write("**Key Factors**:")
                for factor in forecast_data['key_factors']:
                    st.write(f"• {factor}")
        else:
            st.error("No forecast data available")
    else:
        st.info("Click 'Generate Forecast' to create predictions.")

if __name__ == "__main__":
    create_dashboard() 