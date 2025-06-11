#!/usr/bin/env node

/**
 * 👨‍💼 Test Employee Sales Feature
 * This script tests the new employee sales cards in the dashboard
 */

const http = require('http');
const { execSync } = require('child_process');

console.log('👨‍💼 Testing Employee Sales Feature');
console.log('===================================');
console.log('');

// Test both potential ports
const testPorts = [3000, 3001];

for (const port of testPorts) {
  try {
    console.log(`📋 Testing port ${port}...`);
    const response = execSync(`curl -s http://localhost:${port}`, { timeout: 5000 });
    const content = response.toString();
    
    if (content.includes('Sarah Johnson')) {
      console.log(`✅ Port ${port}: Employee data found!`);
      console.log('   📊 Employee Sales Features Detected:');
      
      if (content.includes('Top Employee Sales')) {
        console.log('   ✅ Section header: "Top Employee Sales"');
      }
      
      if (content.includes('Sarah Johnson')) {
        console.log('   ✅ Employee #1: Sarah Johnson');
      }
      
      if (content.includes('Mike Rodriguez')) {
        console.log('   ✅ Employee #2: Mike Rodriguez');
      }
      
      if (content.includes('Emily Chen')) {
        console.log('   ✅ Employee #3: Emily Chen');
      }
      
      if (content.includes('rank-badge')) {
        console.log('   ✅ Rank badges implemented');
      }
      
      if (content.includes('employee-card')) {
        console.log('   ✅ Employee cards CSS classes');
      }
      
      console.log(`   🌐 Dashboard with employees: http://localhost:${port}`);
      console.log('');
      break;
      
    } else if (content.includes('Toast Sales Dashboard')) {
      console.log(`⚠️  Port ${port}: Dashboard running but no employee data`);
    } else {
      console.log(`❌ Port ${port}: No dashboard content`);
    }
    
  } catch (error) {
    console.log(`❌ Port ${port}: Not accessible`);
  }
}

console.log('🎯 Expected Employee Data:');
console.log('  1. Sarah Johnson - $23,456.78 (156 orders)');
console.log('  2. Mike Rodriguez - $21,298.45 (142 orders)');
console.log('  3. Emily Chen - $19,873.22 (134 orders)');
console.log('  4. David Thompson - $18,756.90 (128 orders)');
console.log('  5. Ashley Williams - $17,234.56 (119 orders)');
console.log('');
console.log('🎨 Visual Features:');
console.log('  • Golden rank badges (#1, #2, #3, etc.)');
console.log('  • Employee cards with hover effects');
console.log('  • Sales totals and order counts');
console.log('  • Average order value calculations');
console.log('');
console.log('🎉 Employee sales feature test complete!'); 