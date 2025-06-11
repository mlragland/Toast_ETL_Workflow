import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import App from './App';

// Mock axios before importing
jest.mock('axios', () => ({
  get: jest.fn()
}));

import axios from 'axios';
const mockedAxios = axios as jest.Mocked<typeof axios>;

// Mock data for testing
const mockDashboardData = {
  data: {
    business_metrics: {
      total_orders: 791,
      total_sales: 126408.82,
      avg_order_value: 159.81,
      earliest_date: '2024-06-07',
      latest_date: '2024-06-09',
      unique_dates: 3
    },
    last_updated: '2025-06-11T06:46:31.123456'
  }
};

describe('Toast Sales Dashboard', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('renders loading spinner initially', () => {
    mockedAxios.get.mockImplementation(() => new Promise(() => {})); // Never resolves
    
    render(<App />);
    
    expect(screen.getByTestId('loading-spinner')).toBeInTheDocument();
    expect(screen.getByText('Loading dashboard data...')).toBeInTheDocument();
  });

  test('displays dashboard when API call succeeds', async () => {
    mockedAxios.get.mockResolvedValueOnce(mockDashboardData);
    
    render(<App />);
    
    await waitFor(() => {
      expect(screen.queryByTestId('loading-spinner')).not.toBeInTheDocument();
    });

    expect(screen.getByTestId('dashboard-container')).toBeInTheDocument();
  });

  test('displays dashboard title and subtitle', async () => {
    mockedAxios.get.mockResolvedValueOnce(mockDashboardData);
    
    render(<App />);
    
    await waitFor(() => {
      expect(screen.getByTestId('dashboard-title')).toBeInTheDocument();
    });
    
    expect(screen.getByTestId('dashboard-title')).toHaveTextContent('🍴 Toast Sales Dashboard');
    expect(screen.getByTestId('dashboard-subtitle')).toHaveTextContent('Real-time sales analytics and insights');
  });

  test('displays correct total sales value', async () => {
    mockedAxios.get.mockResolvedValueOnce(mockDashboardData);
    
    render(<App />);
    
    await waitFor(() => {
      expect(screen.getByTestId('total-sales-value')).toHaveTextContent('$126,408.82');
    });
    
    expect(screen.getByTestId('total-sales-card')).toBeInTheDocument();
  });

  test('displays correct total orders value', async () => {
    mockedAxios.get.mockResolvedValueOnce(mockDashboardData);
    
    render(<App />);
    
    await waitFor(() => {
      expect(screen.getByTestId('total-orders-value')).toHaveTextContent('791');
    });
    
    expect(screen.getByTestId('total-orders-card')).toBeInTheDocument();
  });

  test('displays correct average order value', async () => {
    mockedAxios.get.mockResolvedValueOnce(mockDashboardData);
    
    render(<App />);
    
    await waitFor(() => {
      expect(screen.getByTestId('average-order-value')).toHaveTextContent('$159.81');
    });
    
    expect(screen.getByTestId('average-order-card')).toBeInTheDocument();
  });

  test('displays correct unique days value', async () => {
    mockedAxios.get.mockResolvedValueOnce(mockDashboardData);
    
    render(<App />);
    
    await waitFor(() => {
      expect(screen.getByTestId('unique-days-value')).toHaveTextContent('3');
    });
    
    expect(screen.getByTestId('unique-days-card')).toBeInTheDocument();
  });

  test('displays correct date range', async () => {
    mockedAxios.get.mockResolvedValueOnce(mockDashboardData);
    
    render(<App />);
    
    await waitFor(() => {
      expect(screen.getByTestId('date-range')).toHaveTextContent('2024-06-07 to 2024-06-09');
    });
  });

  test('displays summary card', async () => {
    mockedAxios.get.mockResolvedValueOnce(mockDashboardData);
    
    render(<App />);
    
    await waitFor(() => {
      expect(screen.getByTestId('summary-card')).toBeInTheDocument();
    });
    
    expect(screen.getByText('📊 Data Summary')).toBeInTheDocument();
    expect(screen.getByText('Date Range')).toBeInTheDocument();
    expect(screen.getByText('Last Updated')).toBeInTheDocument();
  });

  test('shows error message when API call fails', async () => {
    const errorMessage = 'Network Error';
    mockedAxios.get.mockRejectedValueOnce(new Error(errorMessage));
    
    render(<App />);
    
    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
    });
    
    expect(screen.getByText(errorMessage)).toBeInTheDocument();
    expect(screen.getByTestId('retry-button')).toBeInTheDocument();
  });

  test('retry button works after error', async () => {
    // First call fails
    mockedAxios.get.mockRejectedValueOnce(new Error('Network Error'));
    
    render(<App />);
    
    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
    });
    
    // Second call succeeds
    mockedAxios.get.mockResolvedValueOnce(mockDashboardData);
    
    const retryButton = screen.getByTestId('retry-button');
    fireEvent.click(retryButton);
    
    await waitFor(() => {
      expect(screen.getByTestId('dashboard-container')).toBeInTheDocument();
    });
    
    expect(screen.queryByTestId('error-message')).not.toBeInTheDocument();
  });

  test('refresh button triggers data refetch', async () => {
    mockedAxios.get.mockResolvedValueOnce(mockDashboardData);
    
    render(<App />);
    
    await waitFor(() => {
      expect(screen.getByTestId('dashboard-container')).toBeInTheDocument();
    });
    
    // Clear previous calls and setup next call
    mockedAxios.get.mockClear();
    mockedAxios.get.mockResolvedValueOnce(mockDashboardData);
    
    const refreshButton = screen.getByTestId('refresh-button');
    fireEvent.click(refreshButton);
    
    expect(mockedAxios.get).toHaveBeenCalledWith('http://localhost:8080/api/dashboard/summary');
  });

  test('calls correct API endpoint', async () => {
    mockedAxios.get.mockResolvedValueOnce(mockDashboardData);
    
    render(<App />);
    
    await waitFor(() => {
      expect(mockedAxios.get).toHaveBeenCalledWith('http://localhost:8080/api/dashboard/summary');
    });
  });

  test('handles invalid data format gracefully', async () => {
    const invalidData = { invalid: 'data' };
    mockedAxios.get.mockResolvedValueOnce(invalidData);
    
    render(<App />);
    
    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toBeInTheDocument();
    });
    
    expect(screen.getByText('Invalid data format received')).toBeInTheDocument();
  });

  test('all metric cards have correct CSS classes', async () => {
    mockedAxios.get.mockResolvedValueOnce(mockDashboardData);
    
    render(<App />);
    
    await waitFor(() => {
      expect(screen.getByTestId('total-sales-card')).toBeInTheDocument();
    });
    
    const salesCard = screen.getByTestId('total-sales-card');
    const ordersCard = screen.getByTestId('total-orders-card');
    const averageCard = screen.getByTestId('average-order-card');
    const daysCard = screen.getByTestId('unique-days-card');
    
    expect(salesCard).toHaveClass('metric-card', 'sales-card');
    expect(ordersCard).toHaveClass('metric-card', 'orders-card');
    expect(averageCard).toHaveClass('metric-card', 'average-card');
    expect(daysCard).toHaveClass('metric-card', 'days-card');
  });

  test('currency formatting works correctly', async () => {
    const customData = {
      data: {
        business_metrics: {
          total_orders: 100,
          total_sales: 1234.56,
          avg_order_value: 12.34,
          earliest_date: '2024-01-01',
          latest_date: '2024-01-02',
          unique_dates: 2
        },
        last_updated: '2025-06-11T06:46:31.123456'
      }
    };
    
    mockedAxios.get.mockResolvedValueOnce(customData);
    
    render(<App />);
    
    await waitFor(() => {
      expect(screen.getByTestId('total-sales-value')).toHaveTextContent('$1,234.56');
    });
    
    expect(screen.getByTestId('average-order-value')).toHaveTextContent('$12.34');
  });
});
