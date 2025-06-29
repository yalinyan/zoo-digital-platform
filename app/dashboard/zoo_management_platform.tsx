import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar, PieChart, Pie, Cell } from 'recharts';
import { Search, TrendingUp, DollarSign, BarChart3, Brain, Upload, Download, AlertTriangle, Target, Zap } from 'lucide-react';
import * as math from 'mathjs';

const ZooManagementPlatform = () => {
  const [activeTab, setActiveTab] = useState('dashboard');
  const [aiQuery, setAiQuery] = useState('');
  const [aiResponse, setAiResponse] = useState('');

  // Sample data - in production, this would come from your integrated systems
  const [animalData] = useState([
    { id: 1, name: 'Leo', species: 'African Lion', age: 8, feedCost: 450, projectedCost: 420, actualCost: 465, revenue: 1200, roi: 161 },
    { id: 2, name: 'Mia', species: 'Asian Elephant', age: 12, feedCost: 680, projectedCost: 650, actualCost: 702, revenue: 2100, roi: 199 },
    { id: 3, name: 'Zara', species: 'Siberian Tiger', age: 6, feedCost: 380, projectedCost: 360, actualCost: 395, revenue: 980, roi: 148 },
    { id: 4, name: 'Oscar', species: 'Polar Bear', age: 10, feedCost: 520, projectedCost: 500, actualCost: 535, revenue: 1450, roi: 171 },
    { id: 5, name: 'Bella', species: 'Giant Panda', age: 7, feedCost: 350, projectedCost: 330, actualCost: 368, revenue: 1800, roi: 389 }
  ]);

  const [monthlyData] = useState([
    { month: 'Jan', projected: 12500, actual: 13200, revenue: 28000, visitors: 4200 },
    { month: 'Feb', projected: 11800, actual: 12100, revenue: 25500, visitors: 3800 },
    { month: 'Mar', projected: 13200, actual: 13850, revenue: 32000, visitors: 4800 },
    { month: 'Apr', projected: 14500, actual: 14200, revenue: 35000, visitors: 5200 },
    { month: 'May', projected: 15200, actual: 15800, revenue: 38500, visitors: 5800 },
    { month: 'Jun', projected: 16000, actual: 16500, revenue: 42000, visitors: 6200 }
  ]);

  const [costBreakdown] = useState([
    { category: 'Animal Feed', value: 45, cost: 16500 },
    { category: 'Veterinary Care', value: 25, cost: 9200 },
    { category: 'Facility Maintenance', value: 20, cost: 7300 },
    { category: 'Staff Wages', value: 10, cost: 3700 }
  ]);

  const [recommendations] = useState([
    { type: 'Cost Saving', title: 'Bulk Feed Purchase Optimization', impact: '$2,400/month', description: 'Switch to quarterly bulk orders for elephant feed to reduce costs by 15%' },
    { type: 'Revenue', title: 'Panda Experience Premium Package', impact: '+$3,200/month', description: 'Launch exclusive panda feeding experience during peak hours' },
    { type: 'Efficiency', title: 'Automated Feed Dispensing', impact: '$1,800/month', description: 'Install automated systems for big cats to reduce labor costs' },
    { type: 'Revenue', title: 'Weekend Premium Pricing', impact: '+$4,500/month', description: 'Implement dynamic pricing for weekend visits' }
  ]);

  const COLORS = ['#8884d8', '#82ca9d', '#ffc658', '#ff7300'];

  const handleAiQuery = () => {
    // Simulate AI processing
    setTimeout(() => {
      let response = '';
      const query = aiQuery.toLowerCase();
      
      if (query.includes('cost') && query.includes('save')) {
        response = `Based on current data analysis, I've identified 3 key cost-saving opportunities:

1. **Feed Optimization**: Switch to bulk purchasing for high-consumption animals (elephants, bears). Potential savings: $2,400/month
2. **Veterinary Scheduling**: Consolidate routine checkups to reduce call-out fees. Potential savings: $800/month  
3. **Energy Efficiency**: Upgrade habitat heating systems for cold-weather animals. Potential savings: $1,200/month

Total potential monthly savings: $4,400`;
      } else if (query.includes('roi') || query.includes('revenue')) {
        response = `ROI Analysis Summary:
- **Highest ROI**: Giant Panda (389%) - Premium attraction driving high ticket sales
- **Growth Opportunity**: Siberian Tiger (148%) - Below average, recommend enhanced experience packages
- **Revenue Drivers**: Elephants and Lions consistently generate 150%+ ROI

**Recommendations**: 
1. Expand panda viewing times during peak season
2. Create tiger feeding experiences 
3. Bundle animal encounters for higher per-visitor revenue`;
      } else if (query.includes('animal') || query.includes('feed')) {
        response = `Animal Performance Insights:
- 5 animals currently tracked with average ROI of 214%
- Feed costs trending 5% above projections this quarter
- Bella (Giant Panda) is your star performer with highest revenue generation
- Leo (Lion) showing cost overruns - recommend feed portion review

**Action Items**: Review feed schedules for cost optimization, consider premium experiences for underperforming animals.`;
      } else {
        response = `I can help you analyze zoo operations data. Try asking about:
- "How can we save costs on animal feed?"
- "Which animals have the best ROI?"
- "Show me revenue optimization opportunities"
- "What are our biggest cost drivers?"

I have access to animal tracking, feed costs, visitor data, and financial performance metrics.`;
      }
      
      setAiResponse(response);
    }, 1500);
  };

  const Dashboard = () => (
    <div className="space-y-6">
      {/* KPI Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-gradient-to-r from-blue-500 to-blue-600 text-white p-6 rounded-lg shadow-lg">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-blue-100">Monthly Revenue</p>
              <p className="text-2xl font-bold">$42,000</p>
              <p className="text-sm text-blue-100">+12% vs projected</p>
            </div>
            <DollarSign className="h-8 w-8 text-blue-200" />
          </div>
        </div>
        
        <div className="bg-gradient-to-r from-green-500 to-green-600 text-white p-6 rounded-lg shadow-lg">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-green-100">Cost Efficiency</p>
              <p className="text-2xl font-bold">92%</p>
              <p className="text-sm text-green-100">Above target</p>
            </div>
            <Target className="h-8 w-8 text-green-200" />
          </div>
        </div>
        
        <div className="bg-gradient-to-r from-purple-500 to-purple-600 text-white p-6 rounded-lg shadow-lg">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-purple-100">Average ROI</p>
              <p className="text-2xl font-bold">214%</p>
              <p className="text-sm text-purple-100">Per animal</p>
            </div>
            <TrendingUp className="h-8 w-8 text-purple-200" />
          </div>
        </div>
        
        <div className="bg-gradient-to-r from-orange-500 to-orange-600 text-white p-6 rounded-lg shadow-lg">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-orange-100">Cost Savings</p>
              <p className="text-2xl font-bold">$4,400</p>
              <p className="text-sm text-orange-100">Potential/month</p>
            </div>
            <Zap className="h-8 w-8 text-orange-200" />
          </div>
        </div>
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-white p-6 rounded-lg shadow-lg">
          <h3 className="text-lg font-semibold mb-4">Projected vs Actual Costs</h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={monthlyData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="month" />
              <YAxis />
              <Tooltip formatter={(value) => [`$${value}`, '']} />
              <Line type="monotone" dataKey="projected" stroke="#8884d8" strokeWidth={2} name="Projected" />
              <Line type="monotone" dataKey="actual" stroke="#82ca9d" strokeWidth={2} name="Actual" />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className="bg-white p-6 rounded-lg shadow-lg">
          <h3 className="text-lg font-semibold mb-4">Cost Breakdown</h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={costBreakdown}
                cx="50%"
                cy="50%"
                labelLine={false}
                label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                outerRadius={80}
                fill="#8884d8"
                dataKey="value"
              >
                {costBreakdown.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip formatter={(value, name) => [`${value}%`, name]} />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Revenue Trends */}
      <div className="bg-white p-6 rounded-lg shadow-lg">
        <h3 className="text-lg font-semibold mb-4">Revenue & Visitor Trends</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={monthlyData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="month" />
            <YAxis />
            <Tooltip />
            <Bar dataKey="revenue" fill="#8884d8" name="Revenue ($)" />
            <Bar dataKey="visitors" fill="#82ca9d" name="Visitors" />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );

  const AnimalAnalytics = () => (
    <div className="space-y-6">
      <div className="bg-white p-6 rounded-lg shadow-lg">
        <h3 className="text-lg font-semibold mb-4">Animal Performance Dashboard</h3>
        <div className="overflow-x-auto">
          <table className="min-w-full table-auto">
            <thead>
              <tr className="bg-gray-50">
                <th className="px-4 py-2 text-left">Animal</th>
                <th className="px-4 py-2 text-left">Species</th>
                <th className="px-4 py-2 text-right">Projected Cost</th>
                <th className="px-4 py-2 text-right">Actual Cost</th>
                <th className="px-4 py-2 text-right">Variance</th>
                <th className="px-4 py-2 text-right">Revenue</th>
                <th className="px-4 py-2 text-right">ROI %</th>
                <th className="px-4 py-2 text-center">Status</th>
              </tr>
            </thead>
            <tbody>
              {animalData.map((animal) => {
                const variance = ((animal.actualCost - animal.projectedCost) / animal.projectedCost * 100).toFixed(1);
                const isOverBudget = animal.actualCost > animal.projectedCost;
                
                return (
                  <tr key={animal.id} className="border-b hover:bg-gray-50">
                    <td className="px-4 py-2 font-medium">{animal.name}</td>
                    <td className="px-4 py-2">{animal.species}</td>
                    <td className="px-4 py-2 text-right">${animal.projectedCost}</td>
                    <td className="px-4 py-2 text-right">${animal.actualCost}</td>
                    <td className={`px-4 py-2 text-right ${isOverBudget ? 'text-red-600' : 'text-green-600'}`}>
                      {isOverBudget ? '+' : ''}{variance}%
                    </td>
                    <td className="px-4 py-2 text-right">${animal.revenue}</td>
                    <td className="px-4 py-2 text-right font-semibold">{animal.roi}%</td>
                    <td className="px-4 py-2 text-center">
                      {animal.roi > 200 ? (
                        <span className="px-2 py-1 bg-green-100 text-green-800 rounded-full text-xs">Excellent</span>
                      ) : animal.roi > 150 ? (
                        <span className="px-2 py-1 bg-yellow-100 text-yellow-800 rounded-full text-xs">Good</span>
                      ) : (
                        <span className="px-2 py-1 bg-red-100 text-red-800 rounded-full text-xs">Review</span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      <div className="bg-white p-6 rounded-lg shadow-lg">
        <h3 className="text-lg font-semibold mb-4">ROI Performance Chart</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={animalData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="name" />
            <YAxis />
            <Tooltip formatter={(value) => [`${value}%`, 'ROI']} />
            <Bar dataKey="roi" fill="#8884d8" />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );

  const Recommendations = () => (
    <div className="space-y-6">
      <div className="bg-white p-6 rounded-lg shadow-lg">
        <h3 className="text-lg font-semibold mb-4">AI-Powered Recommendations</h3>
        <div className="grid gap-4">
          {recommendations.map((rec, index) => (
            <div key={index} className="border rounded-lg p-4 hover:shadow-md transition-shadow">
              <div className="flex items-start justify-between">
                <div className="flex-1">
                  <div className="flex items-center gap-2 mb-2">
                    <span className={`px-2 py-1 text-xs rounded-full ${
                      rec.type === 'Cost Saving' ? 'bg-green-100 text-green-800' :
                      rec.type === 'Revenue' ? 'bg-blue-100 text-blue-800' :
                      'bg-purple-100 text-purple-800'
                    }`}>
                      {rec.type}
                    </span>
                    <h4 className="font-semibold">{rec.title}</h4>
                  </div>
                  <p className="text-gray-600 text-sm mb-2">{rec.description}</p>
                </div>
                <div className="text-right">
                  <div className="font-bold text-lg text-green-600">{rec.impact}</div>
                  <button className="mt-2 px-3 py-1 bg-blue-500 text-white text-xs rounded hover:bg-blue-600">
                    Implement
                  </button>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );

  const AIAssistant = () => (
    <div className="space-y-6">
      <div className="bg-gradient-to-r from-purple-500 to-blue-600 text-white p-6 rounded-lg shadow-lg">
        <div className="flex items-center gap-3 mb-4">
          <Brain className="h-8 w-8" />
          <h3 className="text-xl font-semibold">Zoo Intelligence Assistant</h3>
        </div>
        <p className="text-purple-100">Ask me anything about your zoo's performance, costs, revenue optimization, or strategic insights.</p>
      </div>

      <div className="bg-white p-6 rounded-lg shadow-lg">
        <div className="flex gap-4 mb-4">
          <input
            type="text"
            value={aiQuery}
            onChange={(e) => setAiQuery(e.target.value)}
            placeholder="Ask about cost savings, ROI analysis, revenue optimization..."
            className="flex-1 p-3 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            onKeyPress={(e) => e.key === 'Enter' && handleAiQuery()}
          />
          <button
            onClick={handleAiQuery}
            className="px-6 py-3 bg-blue-500 text-white rounded-lg hover:bg-blue-600 flex items-center gap-2"
          >
            <Search className="h-4 w-4" />
            Analyze
          </button>
        </div>

        {aiResponse && (
          <div className="bg-gray-50 p-4 rounded-lg">
            <h4 className="font-semibold mb-2 flex items-center gap-2">
              <Brain className="h-4 w-4 text-purple-500" />
              AI Analysis
            </h4>
            <div className="whitespace-pre-line text-gray-700">{aiResponse}</div>
          </div>
        )}

        <div className="mt-6">
          <h4 className="font-semibold mb-3">Quick Insights</h4>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <button
              onClick={() => {
                setAiQuery('How can we save costs on animal feed?');
                handleAiQuery();
              }}
              className="p-3 text-left border rounded-lg hover:bg-gray-50"
            >
              <div className="font-medium">Cost Optimization</div>
              <div className="text-sm text-gray-600">Analyze feed and operational costs</div>
            </button>
            <button
              onClick={() => {
                setAiQuery('Which animals have the best ROI?');
                handleAiQuery();
              }}
              className="p-3 text-left border rounded-lg hover:bg-gray-50"
            >
              <div className="font-medium">ROI Analysis</div>
              <div className="text-sm text-gray-600">Animal performance insights</div>
            </button>
            <button
              onClick={() => {
                setAiQuery('Show me revenue optimization opportunities');
                handleAiQuery();
              }}
              className="p-3 text-left border rounded-lg hover:bg-gray-50"
            >
              <div className="font-medium">Revenue Growth</div>
              <div className="text-sm text-gray-600">Identify income opportunities</div>
            </button>
            <button
              onClick={() => {
                setAiQuery('What are our biggest cost drivers?');
                handleAiQuery();
              }}
              className="p-3 text-left border rounded-lg hover:bg-gray-50"
            >
              <div className="font-medium">Cost Analysis</div>
              <div className="text-sm text-gray-600">Understand expense patterns</div>
            </button>
          </div>
        </div>
      </div>
    </div>
  );

  const DataIntegration = () => (
    <div className="space-y-6">
      <div className="bg-white p-6 rounded-lg shadow-lg">
        <h3 className="text-lg font-semibold mb-4">Data Integration Hub</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="space-y-4">
            <h4 className="font-semibold">Connected Systems</h4>
            <div className="space-y-3">
              <div className="flex items-center justify-between p-3 border rounded-lg">
                <div>
                  <div className="font-medium">Animal Tracking System</div>
                  <div className="text-sm text-gray-600">Live data sync</div>
                </div>
                <div className="h-3 w-3 bg-green-500 rounded-full"></div>
              </div>
              <div className="flex items-center justify-between p-3 border rounded-lg">
                <div>
                  <div className="font-medium">ERP System (Sales)</div>
                  <div className="text-sm text-gray-600">Real-time updates</div>
                </div>
                <div className="h-3 w-3 bg-green-500 rounded-full"></div>
              </div>
              <div className="flex items-center justify-between p-3 border rounded-lg">
                <div>
                  <div className="font-medium">Purchasing System</div>
                  <div className="text-sm text-gray-600">Daily sync</div>
                </div>
                <div className="h-3 w-3 bg-yellow-500 rounded-full"></div>
              </div>
              <div className="flex items-center justify-between p-3 border rounded-lg">
                <div>
                  <div className="font-medium">Zoo Keeper Feed Plans</div>
                  <div className="text-sm text-gray-600">Manual input required</div>
                </div>
                <div className="h-3 w-3 bg-red-500 rounded-full"></div>
              </div>
            </div>
          </div>

          <div className="space-y-4">
            <h4 className="font-semibold">Quick Data Upload</h4>
            <div className="border-2 border-dashed border-gray-300 rounded-lg p-6 text-center">
              <Upload className="h-12 w-12 text-gray-400 mx-auto mb-4" />
              <p className="text-gray-600 mb-4">Drop CSV files or click to upload</p>
              <button className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600">
                Choose Files
              </button>
            </div>
            <div className="space-y-2">
              <button className="w-full p-2 text-left border rounded hover:bg-gray-50">
                📊 Export Current Dashboard Data
              </button>
              <button className="w-full p-2 text-left border rounded hover:bg-gray-50">
                📈 Generate Monthly Report
              </button>
              <button className="w-full p-2 text-left border rounded hover:bg-gray-50">
                🔄 Sync All Systems
              </button>
            </div>
          </div>
        </div>
      </div>

      <div className="bg-white p-6 rounded-lg shadow-lg">
        <h3 className="text-lg font-semibold mb-4">System Architecture Overview</h3>
        <div className="bg-gray-50 p-4 rounded-lg">
          <div className="text-sm space-y-2">
            <div><strong>Data Pipeline:</strong> Real-time ETL processing with automated validation</div>
            <div><strong>Storage:</strong> Cloud-based data warehouse with backup redundancy</div>
            <div><strong>Analytics:</strong> Machine learning models for cost prediction and optimization</div>
            <div><strong>Security:</strong> End-to-end encryption with role-based access control</div>
            <div><strong>Scalability:</strong> Auto-scaling infrastructure supporting 100x data growth</div>
          </div>
        </div>
      </div>
    </div>
  );

  const tabs = [
    { id: 'dashboard', label: 'Dashboard', icon: BarChart3, component: Dashboard },
    { id: 'animals', label: 'Animal Analytics', icon: TrendingUp, component: AnimalAnalytics },
    { id: 'recommendations', label: 'Recommendations', icon: Target, component: Recommendations },
    { id: 'ai', label: 'AI Assistant', icon: Brain, component: AIAssistant },
    { id: 'integration', label: 'Data Integration', icon: Upload, component: DataIntegration }
  ];

  const ActiveComponent = tabs.find(tab => tab.id === activeTab)?.component || Dashboard;

  return (
    <div className="min-h-screen bg-gray-100">
      {/* Header */}
      <div className="bg-white shadow-lg">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-4">
            <div className="flex items-center space-x-4">
              <div className="h-10 w-10 bg-gradient-to-r from-green-500 to-blue-500 rounded-lg flex items-center justify-center">
                <span className="text-white font-bold text-xl">🦁</span>
              </div>
              <div>
                <h1 className="text-2xl font-bold text-gray-900">Zoo Management Platform</h1>
                <p className="text-sm text-gray-600">Cost Analytics & Strategic Intelligence</p>
              </div>
            </div>
            <div className="flex items-center space-x-3">
              <button className="flex items-center gap-2 px-4 py-2 bg-green-500 text-white rounded-lg hover:bg-green-600">
                <Download className="h-4 w-4" />
                Export Report
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Navigation */}
      <div className="bg-white border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <nav className="flex space-x-1">
            {tabs.map((tab) => {
              const Icon = tab.icon;
              return (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id)}
                  className={`flex items-center gap-2 px-4 py-3 text-sm font-medium border-b-2 transition-colors ${
                    activeTab === tab.id
                      ? 'border-blue-500 text-blue-600 bg-blue-50'
                      : 'border-transparent text-gray-600 hover:text-gray-900 hover:bg-gray-50'
                  }`}
                >
                  <Icon className="h-4 w-4" />
                  {tab.label}
                </button>
              );
            })}
          </nav>
        </div>
      </div>

      {/* Main Content */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <ActiveComponent />
      </div>
    </div>
  );
};

export default ZooManagementPlatform;