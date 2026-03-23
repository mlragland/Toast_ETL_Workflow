import React, { useState, useEffect } from 'react';
import { CalendarIcon, ClockIcon, ChartBarIcon, ExclamationTriangleIcon, CheckCircleIcon } from '@heroicons/react/24/outline';

interface BackfillJob {
  job_id: string;
  start_date: string;
  end_date: string;
  status: 'queued' | 'running' | 'completed' | 'failed';
  created_at: string;
  message: string;
}

interface BackfillPanelProps {
  className?: string;
}

export const BackfillPanel: React.FC<BackfillPanelProps> = ({ className = '' }) => {
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [jobs, setJobs] = useState<BackfillJob[]>([]);
  const [message, setMessage] = useState<{ type: 'success' | 'error', text: string } | null>(null);

  // Load recent backfill jobs
  useEffect(() => {
    loadBackfillJobs();
  }, []);

  const loadBackfillJobs = async () => {
    try {
      const response = await fetch('http://localhost:8080/api/backfill');
      if (response.ok) {
        const data = await response.json();
        if (data.status === 'success' && Array.isArray(data.data)) {
          setJobs(data.data);
        }
      }
    } catch (error) {
      console.error('Failed to load backfill jobs:', error);
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!startDate || !endDate) {
      setMessage({ type: 'error', text: 'Please select both start and end dates' });
      return;
    }

    if (new Date(startDate) > new Date(endDate)) {
      setMessage({ type: 'error', text: 'Start date must be before end date' });
      return;
    }

    setIsSubmitting(true);
    setMessage(null);

    try {
      const response = await fetch('http://localhost:8080/api/backfill', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          start_date: startDate.replace(/-/g, ''),
          end_date: endDate.replace(/-/g, ''),
        }),
      });

      const data = await response.json();

      if (response.ok && data.status === 'success') {
        setMessage({ 
          type: 'success', 
          text: `Backfill job created successfully for ${startDate} to ${endDate}` 
        });
        setStartDate('');
        setEndDate('');
        // Refresh job list
        await loadBackfillJobs();
      } else {
        setMessage({ 
          type: 'error', 
          text: data.message || 'Failed to create backfill job' 
        });
      }
    } catch (error) {
      setMessage({ 
        type: 'error', 
        text: 'Network error. Please try again.' 
      });
    } finally {
      setIsSubmitting(false);
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'completed':
        return <CheckCircleIcon className="h-5 w-5 text-green-500" />;
      case 'failed':
        return <ExclamationTriangleIcon className="h-5 w-5 text-red-500" />;
      case 'running':
        return <ChartBarIcon className="h-5 w-5 text-blue-500 animate-pulse" />;
      case 'queued':
      default:
        return <ClockIcon className="h-5 w-5 text-yellow-500" />;
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed':
        return 'bg-green-100 text-green-800 border-green-200';
      case 'failed':
        return 'bg-red-100 text-red-800 border-red-200';
      case 'running':
        return 'bg-blue-100 text-blue-800 border-blue-200';
      case 'queued':
      default:
        return 'bg-yellow-100 text-yellow-800 border-yellow-200';
    }
  };

  return (
    <div className={`bg-gray-800 rounded-lg border border-gray-700 p-6 ${className}`}>
      <div className="flex items-center mb-6">
        <CalendarIcon className="h-6 w-6 text-blue-400 mr-3" />
        <h3 className="text-lg font-semibold text-white">Historical Data Backfill</h3>
      </div>

      {/* Backfill Form */}
      <form onSubmit={handleSubmit} className="mb-8">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
          <div>
            <label htmlFor="start-date" className="block text-sm font-medium text-gray-300 mb-2">
              Start Date
            </label>
            <input
              type="date"
              id="start-date"
              value={startDate}
              onChange={(e) => setStartDate(e.target.value)}
              className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-md text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              disabled={isSubmitting}
            />
          </div>
          <div>
            <label htmlFor="end-date" className="block text-sm font-medium text-gray-300 mb-2">
              End Date
            </label>
            <input
              type="date"
              id="end-date"
              value={endDate}
              onChange={(e) => setEndDate(e.target.value)}
              className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-md text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              disabled={isSubmitting}
            />
          </div>
        </div>

        <button
          type="submit"
          disabled={isSubmitting || !startDate || !endDate}
          className="w-full md:w-auto px-6 py-2 bg-blue-600 text-white font-medium rounded-md hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 focus:ring-offset-gray-800 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          {isSubmitting ? (
            <span className="flex items-center justify-center">
              <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
              </svg>
              Processing...
            </span>
          ) : (
            'Start Backfill'
          )}
        </button>

        {/* Status Message */}
        {message && (
          <div className={`mt-4 p-3 rounded-md border ${
            message.type === 'success' 
              ? 'bg-green-900 text-green-200 border-green-700' 
              : 'bg-red-900 text-red-200 border-red-700'
          }`}>
            {message.text}
          </div>
        )}
      </form>

      {/* Recent Jobs */}
      <div>
        <h4 className="text-md font-semibold text-white mb-4">Recent Backfill Jobs</h4>
        
        {jobs.length === 0 ? (
          <div className="text-gray-400 text-center py-8">
            No backfill jobs found. Create your first backfill job above.
          </div>
        ) : (
          <div className="space-y-3 max-h-64 overflow-y-auto">
            {jobs.map((job) => (
              <div
                key={job.job_id}
                className="bg-gray-700 rounded-lg p-4 border border-gray-600"
              >
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center space-x-2">
                    {getStatusIcon(job.status)}
                    <span className="text-white font-medium">
                      {job.start_date} → {job.end_date}
                    </span>
                  </div>
                  <span className={`px-2 py-1 rounded-full text-xs font-medium border ${getStatusColor(job.status)}`}>
                    {job.status.toUpperCase()}
                  </span>
                </div>
                <div className="text-sm text-gray-300">
                  Job ID: {job.job_id}
                </div>
                <div className="text-sm text-gray-400">
                  Created: {new Date(job.created_at).toLocaleString()}
                </div>
                {job.message && (
                  <div className="text-sm text-gray-300 mt-1">
                    {job.message}
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}; 