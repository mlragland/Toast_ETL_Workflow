#!/usr/bin/env node

/**
 * 🍴 Toast Dashboard Static Demo
 * This script demonstrates the working static dashboard
 */

const http = require('http');
const { execSync } = require('child_process');

console.log('🍴 Toast Dashboard - Static Demo');
console.log('================================');
console.log('');

// Check if dashboard is running
console.log('📋 Checking Dashboard Status...');
try {
  const response = execSync('curl -s -o /dev/null -w "%{http_code}" http://localhost:3000', { timeout: 5000 });
  const statusCode = response.toString().trim();
  
  if (statusCode === '200') {
    console.log('✅ Dashboard is running on http://localhost:3000');
    console.log('');
    
    console.log('🎯 Static Demo Features:');
    console.log('  💰 Total Sales: $189,673.45');
    console.log('  📋 Total Orders: 1,247');
    console.log('  💳 Average Order Value: $152.17');
    console.log('  📅 Days Tracked: 3');
    console.log('  🧾 Recent Orders: Sample order data');
    console.log('  🏆 Top Items: Sample menu items');
    console.log('  👨‍💼 Top Employee Sales: Performance rankings');
    console.log('');
    
    console.log('✨ Benefits of Static Mode:');
    console.log('  ⚡ No backend dependency');
    console.log('  🚀 Instant loading');
    console.log('  🎨 Full UI demonstration');
    console.log('  📱 Responsive design preview');
    console.log('');
    
    console.log('🌐 Open in browser: http://localhost:3000');
    console.log('');
    console.log('📊 The dashboard showcases:');
    console.log('  • Dark theme UI');
    console.log('  • Colored metric cards');
    console.log('  • Sales analytics layout');
    console.log('  • Recent orders display');
    console.log('  • Top selling items');
    console.log('  • Employee performance rankings');
    console.log('  • Professional styling');
    
  } else {
    console.log('❌ Dashboard not accessible (HTTP ' + statusCode + ')');
    console.log('   Please run: npm start');
  }
} catch (error) {
  console.log('❌ Dashboard not running');
  console.log('   Please run: npm start');
}

console.log('');
console.log('🎉 Static dashboard demo ready!'); 