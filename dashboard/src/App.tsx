import React, { useState, useEffect } from 'react';
import { 
  apiService, 
  handleApiError,
  BusinessMetrics,
  ServerEmployee,
  RecentOrder,
  ServiceSales 
} from './services/api';

function App() {
  // State for dashboard data
  const [businessMetrics, setBusinessMetrics] = useState<BusinessMetrics | null>(null);
  const [employeeData, setEmployeeData] = useState<ServerEmployee[]>([]);
  const [recentOrders, setRecentOrders] = useState<RecentOrder[]>([]);
  const [serviceData, setServiceData] = useState<ServiceSales[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<string>('');

  // Utility functions
  const formatCurrency = (amount: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD'
    }).format(amount);
  };

  const formatNumber = (num: number) => {
    return new Intl.NumberFormat('en-US').format(num);
  };

  // Fetch all dashboard data
  const fetchDashboardData = async () => {
    try {
      setLoading(true);
      setError(null);

      // Fetch all data in parallel
      const [summaryData, serversData, ordersData, salesData] = await Promise.all([
        apiService.getDashboardSummary(),
        apiService.getTopServers(6),
        apiService.getRecentOrders(5),
        apiService.getSalesByService()
      ]);

      // Update state
      setBusinessMetrics(summaryData.data.business_metrics);
      setEmployeeData(serversData);
      setRecentOrders(ordersData);
      setServiceData(salesData);
      setLastUpdated(summaryData.data.last_updated);

    } catch (err) {
      const errorMessage = handleApiError(err);
      setError(errorMessage);
      console.error('Failed to fetch dashboard data:', err);
    } finally {
      setLoading(false);
    }
  };

  // Load data on component mount
  useEffect(() => {
    fetchDashboardData();
  }, []);

  // Refresh data function
  const handleRefresh = () => {
    fetchDashboardData();
  };

  // Loading state
  if (loading) {
    return (
      <div className="dashboard-container" data-testid="dashboard-container">
        <div className="max-w-6xl mx-auto p-6 text-center">
          <div className="loading-spinner">
            <div className="spinner"></div>
            <p className="text-white mt-4">Loading dashboard data...</p>
          </div>
        </div>
      </div>
    );
  }

  // Error state
  if (error) {
    return (
      <div className="dashboard-container" data-testid="dashboard-container">
        <div className="max-w-6xl mx-auto p-6 text-center">
          <div className="error-message">
            <h2 className="text-2xl font-bold text-red-400 mb-4">⚠️ Error Loading Dashboard</h2>
            <p className="text-gray-300 mb-6">{error}</p>
            <button 
              onClick={handleRefresh}
              className="bg-blue-600 hover:bg-blue-700 text-white px-6 py-2 rounded font-semibold"
            >
              Retry
            </button>
          </div>
        </div>
      </div>
    );
  }

  // Main dashboard render
  return (
    <div className="dashboard-container" data-testid="dashboard-container">
      <div className="max-w-6xl mx-auto p-6">
        {/* Header */}
        <div className="mb-8 text-center">
          <h1 className="text-3xl font-bold text-white mb-2" data-testid="dashboard-title">
            🍴 Toast Sales Dashboard
          </h1>
          <p className="text-gray-400" data-testid="dashboard-subtitle">
            Real-time sales analytics and insights
          </p>
          <button 
            onClick={handleRefresh}
            className="mt-2 bg-green-600 hover:bg-green-700 text-white px-4 py-1 rounded text-sm"
          >
            🔄 Refresh Data
          </button>
        </div>

        {/* Metrics Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
          {/* Total Sales */}
          <div className="metric-card sales-card" data-testid="total-sales-card">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-green-400 text-sm font-semibold">💰 TOTAL SALES</p>
                <p className="text-2xl font-bold text-white" data-testid="total-sales-value">
                  {businessMetrics ? formatCurrency(businessMetrics.total_sales) : '$0.00'}
                </p>
              </div>
            </div>
          </div>

          {/* Total Orders */}
          <div className="metric-card orders-card" data-testid="total-orders-card">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-blue-400 text-sm font-semibold">📋 TOTAL ORDERS</p>
                <p className="text-2xl font-bold text-white" data-testid="total-orders-value">
                  {businessMetrics ? formatNumber(businessMetrics.total_orders) : '0'}
                </p>
              </div>
            </div>
          </div>

          {/* Average Order Value */}
          <div className="metric-card average-card" data-testid="average-order-card">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-orange-400 text-sm font-semibold">💳 AVG ORDER VALUE</p>
                <p className="text-2xl font-bold text-white" data-testid="average-order-value">
                  {businessMetrics ? formatCurrency(businessMetrics.avg_order_value) : '$0.00'}
                </p>
              </div>
            </div>
          </div>

          {/* Days Tracked */}
          <div className="metric-card days-card" data-testid="unique-days-card">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-purple-400 text-sm font-semibold">📅 DAYS TRACKED</p>
                <p className="text-2xl font-bold text-white" data-testid="unique-days-value">
                  {businessMetrics ? businessMetrics.unique_dates : 0}
                </p>
              </div>
            </div>
          </div>
        </div>

        {/* Summary Information */}
        <div className="summary-card" data-testid="summary-card">
          <h2 className="text-xl font-semibold text-white mb-4">📊 Data Summary</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <h3 className="text-lg font-semibold text-gray-400 mb-2">Date Range</h3>
              <p className="text-white" data-testid="date-range">
                {businessMetrics 
                  ? `${businessMetrics.earliest_date} to ${businessMetrics.latest_date}` 
                  : 'No data available'
                }
              </p>
            </div>
            <div>
              <h3 className="text-lg font-semibold text-gray-400 mb-2">Last Updated</h3>
              <p className="text-white" data-testid="last-updated">
                {lastUpdated 
                  ? new Date(lastUpdated).toLocaleString() 
                  : 'Never'
                }
              </p>
            </div>
          </div>
        </div>

        {/* Additional Live Data */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mt-8">
          {/* Recent Orders */}
          <div className="summary-card">
            <h2 className="text-xl font-semibold text-white mb-4">🧾 Recent Orders</h2>
            <div className="space-y-3">
              {recentOrders.length > 0 ? (
                recentOrders.map((order) => (
                  <div key={order.order_id} className="flex justify-between items-center">
                    <div className="flex flex-col">
                      <span className="text-gray-400">#{order.order_number}</span>
                      <span className="text-gray-500 text-xs">{order.server}</span>
                    </div>
                    <span className="text-green-400 font-semibold">
                      {formatCurrency(order.total)}
                    </span>
                  </div>
                ))
              ) : (
                <p className="text-gray-400">No recent orders available</p>
              )}
            </div>
          </div>

          {/* Sales by Service */}
          <div className="summary-card">
            <h2 className="text-xl font-semibold text-white mb-4">🏆 Sales by Service</h2>
            <div className="space-y-3">
              {serviceData.length > 0 ? (
                serviceData.slice(0, 4).map((service) => (
                  <div key={service.service} className="flex justify-between items-center">
                    <div className="flex flex-col">
                      <span className="text-gray-400">{service.service}</span>
                      <span className="text-gray-500 text-xs">{service.order_count} orders</span>
                    </div>
                    <span className="text-cyan-400 font-semibold">
                      {formatCurrency(service.total_sales)}
                    </span>
                  </div>
                ))
              ) : (
                <p className="text-gray-400">No service data available</p>
              )}
            </div>
          </div>
        </div>

        {/* Top Employee Performance - 3x2 Grid */}
        <div className="mt-8">
          <h2 className="text-2xl font-semibold text-white mb-6 text-center">👨‍💼 Top Server Performance</h2>
          <div className="employee-grid">
            {employeeData.length > 0 ? (
              employeeData.map((server, index) => (
                <div key={server.server} className="employee-tile">
                  <div className="employee-tile-header">
                    <div className="employee-rank">
                      #{index + 1}
                    </div>
                    <div className="employee-name">
                      {server.server}
                    </div>
                  </div>
                  
                  <div className="employee-stats">
                    <div className="stat-item primary-stat">
                      <div className="stat-label">Total Sales</div>
                      <div className="stat-value sales-value">
                        {formatCurrency(server.total_sales)}
                      </div>
                    </div>
                    
                    <div className="employee-metrics">
                      <div className="metric-item">
                        <span className="metric-label">Orders</span>
                        <span className="metric-value">{formatNumber(server.order_count)}</span>
                      </div>
                      <div className="metric-item">
                        <span className="metric-label">Avg/Order</span>
                        <span className="metric-value">{formatCurrency(server.avg_order_value)}</span>
                      </div>
                    </div>
                  </div>
                  
                  <div className="employee-footer">
                    <div className="performance-indicator">
                      <span className="indicator-dot"></span>
                      <span className="indicator-text">Active</span>
                    </div>
                  </div>
                </div>
              ))
            ) : (
              <div className="col-span-full text-center text-gray-400 py-8">
                No server performance data available
              </div>
            )}
          </div>
        </div>

        {/* Status Banner */}
        <div className="mt-8 text-center">
          <div className="bg-green-600 px-6 py-2 rounded inline-block">
            <span className="text-white font-semibold">
              🔄 Live Data - Connected to Backend
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;