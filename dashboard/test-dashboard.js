// Integration test script for Toast Dashboard
// This script tests the dashboard by making actual API calls

const axios = require('axios');

const API_BASE_URL = 'http://localhost:8080';

async function testDashboard() {
  console.log('🧪 Starting Toast Dashboard Integration Tests...\n');

  try {
    // Test 1: Health Check
    console.log('1️⃣ Testing Backend Health...');
    const healthResponse = await axios.get(`${API_BASE_URL}/health`);
    console.log(`✅ Backend Health: ${healthResponse.data.status}`);
    console.log(`   Database: ${healthResponse.data.database}`);
    console.log(`   Timestamp: ${healthResponse.data.timestamp}\n`);

    // Test 2: Dashboard Summary API
    console.log('2️⃣ Testing Dashboard Summary API...');
    const summaryResponse = await axios.get(`${API_BASE_URL}/api/dashboard/summary`);
    
    if (summaryResponse.data && summaryResponse.data.data) {
      const metrics = summaryResponse.data.data.business_metrics;
      console.log('✅ Dashboard API Response:');
      console.log(`   📊 Total Sales: $${metrics.total_sales.toLocaleString()}`);
      console.log(`   📋 Total Orders: ${metrics.total_orders.toLocaleString()}`);
      console.log(`   💳 Average Order Value: $${metrics.avg_order_value.toFixed(2)}`);
      console.log(`   📅 Date Range: ${metrics.earliest_date} to ${metrics.latest_date}`);
      console.log(`   🗓️  Unique Days: ${metrics.unique_dates}`);
      console.log(`   🕒 Last Updated: ${summaryResponse.data.data.last_updated}\n`);
    } else {
      throw new Error('Invalid API response format');
    }

    // Test 3: Validate Data Types
    console.log('3️⃣ Validating Data Types...');
    const metrics = summaryResponse.data.data.business_metrics;
    
    const validations = [
      { field: 'total_sales', value: metrics.total_sales, type: 'number' },
      { field: 'total_orders', value: metrics.total_orders, type: 'number' },
      { field: 'avg_order_value', value: metrics.avg_order_value, type: 'number' },
      { field: 'earliest_date', value: metrics.earliest_date, type: 'string' },
      { field: 'latest_date', value: metrics.latest_date, type: 'string' },
      { field: 'unique_dates', value: metrics.unique_dates, type: 'number' }
    ];

    let validationErrors = 0;
    validations.forEach(({ field, value, type }) => {
      if (typeof value !== type) {
        console.log(`❌ ${field}: Expected ${type}, got ${typeof value}`);
        validationErrors++;
      } else {
        console.log(`✅ ${field}: ${type} ✓`);
      }
    });

    if (validationErrors === 0) {
      console.log('✅ All data types are valid!\n');
    } else {
      console.log(`❌ ${validationErrors} validation errors found!\n`);
    }

    // Test 4: Data Reasonableness
    console.log('4️⃣ Testing Data Reasonableness...');
    const reasonableness = [
      { check: 'Total Sales > 0', result: metrics.total_sales > 0 },
      { check: 'Total Orders > 0', result: metrics.total_orders > 0 },
      { check: 'Average Order Value > 0', result: metrics.avg_order_value > 0 },
      { check: 'Unique Days > 0', result: metrics.unique_dates > 0 },
      { check: 'Average = Total Sales / Total Orders', result: Math.abs((metrics.total_sales / metrics.total_orders) - metrics.avg_order_value) < 0.01 }
    ];

    reasonableness.forEach(({ check, result }) => {
      console.log(`${result ? '✅' : '❌'} ${check}`);
    });

    // Test 5: Frontend Connectivity Test
    console.log('\n5️⃣ Testing Frontend Connectivity...');
    try {
      const frontendResponse = await axios.get('http://localhost:3000', { timeout: 5000 });
      if (frontendResponse.status === 200) {
        console.log('✅ Frontend is accessible on port 3000');
        if (frontendResponse.data.includes('Toast') || frontendResponse.data.includes('dashboard')) {
          console.log('✅ Frontend appears to be the correct dashboard application');
        } else {
          console.log('⚠️ Frontend may not be the dashboard application');
        }
      }
    } catch (frontendError) {
      console.log('❌ Frontend is not accessible on port 3000');
      console.log('   Make sure to run: npm start');
    }

    console.log('\n🎉 Dashboard Integration Tests Complete!');
    console.log('\n📋 Summary:');
    console.log(`   Backend: ${healthResponse.data.status}`);
    console.log(`   API Endpoints: Working`);
    console.log(`   Data Validation: ${validationErrors === 0 ? 'Passed' : 'Failed'}`);
    console.log(`   Sample Data: $${metrics.total_sales.toLocaleString()} in sales, ${metrics.total_orders} orders`);

  } catch (error) {
    console.error('❌ Test Failed:', error.message);
    
    if (error.code === 'ECONNREFUSED') {
      console.log('\n💡 Troubleshooting:');
      console.log('1. Make sure the backend server is running on port 8080');
      console.log('   Command: cd .. && python start_backend.py');
      console.log('2. Make sure you have the required environment variables:');
      console.log('   export PROJECT_ID=toast-analytics-444116');
      console.log('   export DATASET_ID=toast_analytics');
    }
    
    process.exit(1);
  }
}

// Run the tests
testDashboard(); 