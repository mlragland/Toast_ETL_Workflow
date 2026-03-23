# LOV3 Houston - Looker Studio Dashboard Guide

Connect your BigQuery analytics views to Looker Studio for executive reporting.

---

## Table of Contents

1. [Accessing Looker Studio](#1-accessing-looker-studio)
2. [Connecting to BigQuery](#2-connecting-to-bigquery)
3. [Adding Data Sources](#3-adding-data-sources)
4. [Recommended Charts by View](#4-recommended-charts-by-view)
5. [Calculated Fields](#5-calculated-fields)
6. [Filters & Controls](#6-filters--controls)
7. [Dashboard Layout](#7-dashboard-layout)
8. [Sharing & Scheduling](#8-sharing--scheduling)

---

## 1. Accessing Looker Studio

### Step 1: Navigate to Looker Studio
1. Go to [https://lookerstudio.google.com](https://lookerstudio.google.com)
2. Sign in with your Google account (use the same account that has BigQuery access)
3. Click **"Create"** → **"Report"**

### Step 2: Verify Permissions
Ensure your Google account has:
- `BigQuery Data Viewer` role on the `toast-analytics-444116` project
- `BigQuery Job User` role to run queries

To check/add permissions:
```bash
gcloud projects add-iam-policy-binding toast-analytics-444116 \
  --member="user:your-email@domain.com" \
  --role="roles/bigquery.dataViewer"
```

---

## 2. Connecting to BigQuery

### Step 1: Add BigQuery Connector
1. In your new report, click **"Add data"**
2. Search for **"BigQuery"** in the connectors list
3. Click **"BigQuery"** (Google Connector)

### Step 2: Authorize Access
1. Click **"Authorize"** when prompted
2. Select your Google account
3. Allow Looker Studio to access BigQuery

### Step 3: Select Project and Dataset
1. **Project:** `toast-analytics-444116`
2. **Dataset:** `toast_analytics`
3. **Table:** Select the view you want to add

### Step 4: Configure Connection
1. Leave **"Use query results caching"** enabled for better performance
2. Click **"Add"** to add the data source

---

## 3. Adding Data Sources

Add each of these views as a separate data source:

| Data Source Name | BigQuery View | Primary Use |
|------------------|---------------|-------------|
| Daily Revenue | `toast_analytics.daily_revenue_summary` | Revenue trends |
| Server Performance | `toast_analytics.server_performance` | Staff metrics |
| Menu Performance | `toast_analytics.menu_item_performance` | Product analysis |
| Hourly Patterns | `toast_analytics.hourly_sales_pattern` | Traffic analysis |
| Void Analysis | `toast_analytics.void_analysis` | Loss prevention |
| Payment Mix | `toast_analytics.payment_mix` | Payment trends |

### Repeat for Each View:
1. Click **"Resource"** → **"Manage added data sources"**
2. Click **"Add a data source"**
3. Select BigQuery → `toast_analytics` → [view name]
4. Click **"Add"** → **"Add to Report"**

---

## 4. Recommended Charts by View

### 4.1 Daily Revenue Summary

#### Chart 1: Revenue Trend Line
- **Type:** Time Series Chart
- **Dimension:** `processing_date`
- **Metrics:** `total_collected`, `net_sales`
- **Style:** Dual axis, show trend line

#### Chart 2: KPI Scorecards (4 cards)
- **Type:** Scorecard with comparison
- **Metrics:**
  - `SUM(total_collected)` - Total Revenue
  - `SUM(order_count)` - Orders
  - `AVG(avg_check)` - Avg Check
  - `SUM(total_guests)` - Guests
- **Comparison:** Previous period

#### Chart 3: Day of Week Performance
- **Type:** Bar Chart
- **Dimension:** `day_of_week`
- **Metric:** `SUM(total_collected)`
- **Sort:** Custom (Mon-Sun)

#### Chart 4: Monthly Comparison
- **Type:** Combo Chart (bars + line)
- **Dimension:** `month`
- **Metrics:** `SUM(net_sales)` (bars), `AVG(avg_check)` (line)

#### Chart 5: Discount Trend
- **Type:** Area Chart
- **Dimension:** `processing_date`
- **Metrics:** `SUM(discounts)`, `AVG(discount_pct)`

---

### 4.2 Server Performance

#### Chart 1: Top Servers Leaderboard
- **Type:** Table with Heatmap
- **Dimensions:** `server`, `sales_rank`
- **Metrics:** `total_sales`, `avg_check`, `tip_pct`, `void_rate`
- **Sort:** `sales_rank` ascending
- **Rows:** 15

#### Chart 2: Server Sales Distribution
- **Type:** Bar Chart (horizontal)
- **Dimension:** `server`
- **Metric:** `total_sales`
- **Sort:** Descending
- **Limit:** Top 10

#### Chart 3: Efficiency Scatter Plot
- **Type:** Scatter Chart
- **Dimension:** `server`
- **X-Axis:** `total_checks`
- **Y-Axis:** `avg_check`
- **Bubble Size:** `total_sales`

#### Chart 4: Risk Analysis Table
- **Type:** Table with conditional formatting
- **Dimension:** `server`
- **Metrics:** `discount_pct`, `void_rate`, `tip_pct`
- **Conditional Formatting:**
  - `discount_pct` > 10% → Red
  - `void_rate` > 5% → Red
  - `tip_pct` < 5% → Yellow

---

### 4.3 Menu Item Performance

#### Chart 1: Top Items by Revenue
- **Type:** Bar Chart
- **Dimension:** `menu_item`
- **Metric:** `net_revenue`
- **Filter:** `revenue_rank` <= 20

#### Chart 2: Category Breakdown
- **Type:** Pie Chart or Treemap
- **Dimension:** `menu_group`
- **Metric:** `SUM(net_revenue)`

#### Chart 3: Price vs Volume Analysis
- **Type:** Scatter Chart
- **X-Axis:** `avg_price`
- **Y-Axis:** `total_quantity`
- **Bubble Size:** `net_revenue`
- **Dimension:** `menu_item`

#### Chart 4: Item Performance Table
- **Type:** Table with bars
- **Dimensions:** `menu_item`, `menu_group`
- **Metrics:** `net_revenue`, `total_quantity`, `avg_price`, `discount_pct`
- **Data Bars:** On `net_revenue`

---

### 4.4 Hourly Sales Pattern

#### Chart 1: Heatmap - Hour vs Day
- **Type:** Pivot Table with Heatmap
- **Row:** `day_of_week`
- **Column:** `hour_of_day`
- **Metric:** `SUM(total_revenue)`
- **Style:** Heatmap coloring

#### Chart 2: Hourly Revenue by Daypart
- **Type:** Stacked Bar Chart
- **Dimension:** `hour_of_day`
- **Breakdown:** `daypart`
- **Metric:** `total_revenue`

#### Chart 3: Peak Hours Line Chart
- **Type:** Line Chart
- **Dimension:** `hour_of_day`
- **Metrics:** `avg_checks_per_day`, `avg_check`
- **Breakdown:** `day_of_week` (filter to weekends)

---

### 4.5 Void Analysis

#### Chart 1: Void Rate by Server
- **Type:** Bar Chart with target line
- **Dimension:** `server`
- **Metric:** `AVG(void_rate)`
- **Reference Line:** 3% (acceptable threshold)

#### Chart 2: Void Trend Over Time
- **Type:** Time Series
- **Dimension:** `processing_date`
- **Metric:** `SUM(voided_orders)`, `SUM(voided_amount)`

#### Chart 3: Void Alerts Table
- **Type:** Table
- **Filter:** `void_rate` > 5%
- **Dimensions:** `server`, `processing_date`, `day_of_week`
- **Metrics:** `voided_orders`, `void_rate`, `voided_amount`

---

### 4.6 Payment Mix

#### Chart 1: Payment Type Pie Chart
- **Type:** Pie Chart
- **Dimension:** `payment_type`
- **Metric:** `SUM(total_collected)`

#### Chart 2: Card Type Breakdown
- **Type:** Donut Chart
- **Dimension:** `card_type`
- **Metric:** `SUM(total_collected)`
- **Filter:** `payment_type` = "Credit"

#### Chart 3: Tip Rate by Payment Type
- **Type:** Bar Chart
- **Dimension:** `payment_type`
- **Metric:** `AVG(tip_pct)`

#### Chart 4: Payment Trends
- **Type:** Stacked Area Chart
- **Dimension:** `processing_date` (aggregated to week)
- **Breakdown:** `payment_type`
- **Metric:** `SUM(total_collected)`

---

## 5. Calculated Fields

Add these calculated fields to enhance your reports:

### In Daily Revenue Summary:

```
// Revenue vs Last Week
CASE
  WHEN processing_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  THEN total_collected
  ELSE 0
END
```

```
// Weekend Flag
CASE
  WHEN day_of_week IN ('Saturday', 'Sunday', 'Friday')
  THEN 'Weekend'
  ELSE 'Weekday'
END
```

```
// Revenue Category
CASE
  WHEN total_collected >= 40000 THEN 'Excellent'
  WHEN total_collected >= 30000 THEN 'Good'
  WHEN total_collected >= 20000 THEN 'Average'
  ELSE 'Below Target'
END
```

### In Server Performance:

```
// Performance Score (composite)
(total_sales / 10000) + (tip_pct * 2) - (void_rate * 5) - (discount_pct * 3)
```

```
// Server Tier
CASE
  WHEN sales_rank <= 5 THEN 'Top Performer'
  WHEN sales_rank <= 15 THEN 'Strong'
  WHEN sales_rank <= 30 THEN 'Average'
  ELSE 'Needs Coaching'
END
```

```
// Risk Flag
CASE
  WHEN discount_pct > 15 OR void_rate > 8 THEN 'High Risk'
  WHEN discount_pct > 10 OR void_rate > 5 THEN 'Monitor'
  ELSE 'Normal'
END
```

### In Menu Item Performance:

```
// Profitability Tier
CASE
  WHEN revenue_rank <= 10 THEN 'Star'
  WHEN revenue_rank <= 30 THEN 'Performer'
  WHEN revenue_rank <= 100 THEN 'Standard'
  ELSE 'Review'
END
```

```
// Discount Impact
CASE
  WHEN discount_pct > 20 THEN 'Heavy Discounting'
  WHEN discount_pct > 10 THEN 'Moderate'
  ELSE 'Minimal'
END
```

---

## 6. Filters & Controls

### Global Date Range Filter
1. Click **"Add a control"** → **"Date range control"**
2. Apply to all charts
3. Default: Last 30 days

### Page-Level Filters

#### Executive Dashboard:
- Date Range (required)
- Day of Week (multi-select)

#### Server Analysis:
- Date Range
- Server (dropdown, searchable)
- Performance Tier (calculated field)

#### Menu Analysis:
- Date Range
- Menu Group (dropdown)
- Revenue Rank slider (1-100)

#### Operations:
- Date Range
- Day of Week
- Hour of Day (slider)
- Daypart (dropdown)

### Interactive Filters
Add these as dropdown controls:

| Filter | Source Field | Type |
|--------|--------------|------|
| Time Period | `processing_date` | Date Range |
| Day | `day_of_week` | Multi-select |
| Server | `server` | Searchable Dropdown |
| Menu Category | `menu_group` | Dropdown |
| Payment Type | `payment_type` | Checkbox |

---

## 7. Dashboard Layout

### Recommended Page Structure

```
┌─────────────────────────────────────────────────────────────────┐
│                    LOV3 HOUSTON DASHBOARD                       │
│  [Date Range Filter]  [Day Filter]  [Compare: Previous Period]  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  PAGE 1: EXECUTIVE SUMMARY                                      │
│  ┌─────────┬─────────┬─────────┬─────────┐                     │
│  │ Revenue │ Orders  │Avg Check│ Guests  │  ← KPI Scorecards   │
│  │ $XXX,XXX│  X,XXX  │  $XX.XX │  X,XXX  │                     │
│  └─────────┴─────────┴─────────┴─────────┘                     │
│  ┌─────────────────────────────────────────┐ ┌───────────────┐ │
│  │                                         │ │  Day of Week  │ │
│  │     30-Day Revenue Trend (Line)         │ │  Performance  │ │
│  │                                         │ │   (Bar Chart) │ │
│  └─────────────────────────────────────────┘ └───────────────┘ │
│  ┌─────────────────────┐ ┌─────────────────┐ ┌───────────────┐ │
│  │   Top 5 Servers     │ │  Top 10 Items   │ │ Payment Mix   │ │
│  │     (Table)         │ │   (Bar Chart)   │ │ (Pie Chart)   │ │
│  └─────────────────────┘ └─────────────────┘ └───────────────┘ │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  PAGE 2: SERVER PERFORMANCE                                     │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  Server Leaderboard (Full Table with Heatmap)               ││
│  │  Columns: Rank, Server, Sales, Checks, Avg Check, Tips,     ││
│  │           Void Rate, Discount Rate, Risk Flag               ││
│  └─────────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────┐ ┌─────────────────────────────┐│
│  │  Sales Distribution        │ │  Efficiency Scatter Plot    ││
│  │  (Horizontal Bar)          │ │  (Checks vs Avg Check)      ││
│  └─────────────────────────────┘ └─────────────────────────────┘│
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  PAGE 3: MENU ANALYSIS                                          │
│  ┌───────────────┐ ┌───────────────────────────────────────────┐│
│  │ Category Mix  │ │  Top 20 Items by Revenue (Bar Chart)      ││
│  │ (Treemap)     │ │                                           ││
│  └───────────────┘ └───────────────────────────────────────────┘│
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  Full Item Table (Searchable, Sortable)                     ││
│  │  Columns: Item, Category, Qty, Revenue, Avg Price, Rank     ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  PAGE 4: OPERATIONS & TRAFFIC                                   │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  Heatmap: Revenue by Hour and Day of Week                   ││
│  │  (Rows: Days, Columns: Hours 6AM-3AM)                       ││
│  └─────────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────┐ ┌─────────────────────────────┐│
│  │  Hourly Traffic Pattern    │ │  Daypart Breakdown          ││
│  │  (Line Chart)              │ │  (Stacked Bar)              ││
│  └─────────────────────────────┘ └─────────────────────────────┘│
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  PAGE 5: LOSS PREVENTION                                        │
│  ┌─────────┬─────────┬─────────┐                               │
│  │ Voids   │ Discounts│ Comps  │  ← Alert Scorecards          │
│  │ $X,XXX  │ $XX,XXX │ $X,XXX │                               │
│  └─────────┴─────────┴─────────┘                               │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  High-Risk Server Table (Filtered: Risk Flag = High)        ││
│  │  Columns: Server, Void Rate, Discount %, Voided $, Comp $   ││
│  └─────────────────────────────────────────────────────────────┘│
│  ┌─────────────────────────────┐ ┌─────────────────────────────┐│
│  │  Void Trend (Time Series)  │ │  Discount by Server (Bar)   ││
│  └─────────────────────────────┘ └─────────────────────────────┘│
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Color Theme (LOV3 Branding)
```
Primary:    #E91E63 (Pink/Magenta)
Secondary:  #9C27B0 (Purple)
Accent:     #FF9800 (Orange)
Success:    #4CAF50 (Green)
Warning:    #FFC107 (Amber)
Danger:     #F44336 (Red)
Background: #FAFAFA (Light Gray)
Text:       #212121 (Dark Gray)
```

---

## 8. Sharing & Scheduling

### Share the Dashboard

1. Click **"Share"** button (top right)
2. Add email addresses of viewers
3. Set permissions:
   - **Can View** - Read-only access
   - **Can Edit** - Full editing rights

### Schedule Email Delivery

1. Click **"Share"** → **"Schedule email delivery"**
2. Configure:
   - **Recipients:** Executive team emails
   - **Frequency:** Weekly (Monday 8 AM)
   - **Pages:** All or specific pages
   - **Format:** PDF attachment

### Embed in Website (Optional)

1. Click **"File"** → **"Embed report"**
2. Copy the iframe code
3. Paste into your internal portal

---

## Quick Start Checklist

- [ ] Access Looker Studio and create new report
- [ ] Connect to BigQuery project `toast-analytics-444116`
- [ ] Add all 6 data sources from `toast_analytics` dataset
- [ ] Create Page 1: Executive Summary with KPIs and trends
- [ ] Create Page 2: Server Performance leaderboard
- [ ] Create Page 3: Menu Analysis with top items
- [ ] Create Page 4: Hourly heatmap and traffic patterns
- [ ] Create Page 5: Loss Prevention alerts
- [ ] Add global date range filter
- [ ] Add calculated fields for risk flags and tiers
- [ ] Apply LOV3 brand colors
- [ ] Share with stakeholders
- [ ] Schedule weekly email delivery

---

## Support

For BigQuery access issues:
```bash
gcloud auth login
gcloud config set project toast-analytics-444116
```

For data questions, query the views directly:
```sql
SELECT * FROM toast_analytics.daily_revenue_summary
WHERE processing_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY processing_date DESC
```

---

*Dashboard Guide for LOV3 Houston - Toast Analytics Pipeline*
