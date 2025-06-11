import axios from 'axios';

// API Configuration
const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8080';

// Create axios instance with default config
const apiClient = axios.create({
  baseURL: API_BASE_URL,
  timeout: 10000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Types for API responses
export interface BusinessMetrics {
  total_orders: number;
  total_sales: number;
  avg_order_value: number;
  earliest_date: string;
  latest_date: string;
  unique_dates: number;
}

export interface DashboardSummary {
  status: string;
  data: {
    total_rows: number;
    table_stats: Record<string, any>;
    business_metrics: BusinessMetrics;
    last_updated: string;
  };
}

export interface ServerEmployee {
  server: string;
  order_count: number;
  total_sales: number;
  avg_order_value: number;
}

export interface RecentOrder {
  order_id: string;
  order_number: string;
  opened: string;
  server: string;
  total: number;
  revenue_center: string;
  service: string;
  guest_count: number;
}

export interface ServiceSales {
  service: string;
  order_count: number;
  total_sales: number;
  avg_order_value: number;
}

// API Service Class
class ApiService {
  // Health check
  async healthCheck(): Promise<any> {
    const response = await apiClient.get('/health');
    return response.data;
  }

  // Get dashboard summary with business metrics
  async getDashboardSummary(): Promise<DashboardSummary> {
    const response = await apiClient.get<DashboardSummary>('/api/dashboard/summary');
    return response.data;
  }

  // Get top servers (employees) by sales
  async getTopServers(limit: number = 6): Promise<ServerEmployee[]> {
    const response = await apiClient.get<{
      status: string;
      data: ServerEmployee[];
    }>(`/api/analytics/top-servers?limit=${limit}`);
    
    if (response.data.status === 'success') {
      return response.data.data;
    }
    throw new Error('Failed to fetch top servers data');
  }

  // Get recent orders
  async getRecentOrders(limit: number = 5): Promise<RecentOrder[]> {
    const response = await apiClient.get<{
      status: string;
      data: RecentOrder[];
    }>(`/api/orders/recent?limit=${limit}`);
    
    if (response.data.status === 'success') {
      return response.data.data;
    }
    throw new Error('Failed to fetch recent orders');
  }

  // Get sales by service
  async getSalesByService(): Promise<ServiceSales[]> {
    const response = await apiClient.get<{
      status: string;
      data: ServiceSales[];
    }>('/api/analytics/sales-by-service');
    
    if (response.data.status === 'success') {
      return response.data.data;
    }
    throw new Error('Failed to fetch sales by service');
  }
}

// Export singleton instance
export const apiService = new ApiService();

// Error handler utility
export const handleApiError = (error: any): string => {
  if (error.response) {
    // Server responded with error status
    return error.response.data?.message || `Server error: ${error.response.status}`;
  } else if (error.request) {
    // Network error
    return 'Network error: Unable to connect to server. Please check if the backend is running.';
  } else {
    // Other error
    return error.message || 'An unexpected error occurred';
  }
}; 