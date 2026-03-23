"""
Weekly Report Generator for LOV3 Houston
Generates and sends weekly summary reports via email using SendGrid
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from google.cloud import bigquery
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content

from config import (
    PROJECT_ID, DATASET_ID, BUSINESS_DAY_SQL, BUSINESS_DOW_SQL,
    GRAT_RETAIN_PCT, GRAT_PASSTHROUGH_PCT, REPORT_EMAIL,
)
from services import SecretManager

logger = logging.getLogger(__name__)


class WeeklyReportGenerator:
    """Generates and sends weekly summary reports via email"""

    def __init__(self):
        self.bq_client = bigquery.Client(project=PROJECT_ID)
        self.secret_manager = SecretManager(PROJECT_ID)

    def get_week_dates(self, week_ending: str = None) -> Tuple[str, str]:
        """
        Calculate the Monday-Sunday date range for the prior week.

        Args:
            week_ending: Optional date string (YYYYMMDD) for the Sunday ending the week.
                        Defaults to last Sunday.

        Returns:
            Tuple of (monday_date, sunday_date) as YYYY-MM-DD strings
        """
        if week_ending:
            end_date = datetime.strptime(week_ending, "%Y%m%d")
        else:
            # Find last Sunday
            today = datetime.now()
            days_since_sunday = (today.weekday() + 1) % 7
            if days_since_sunday == 0:
                days_since_sunday = 7  # If today is Sunday, go to previous Sunday
            end_date = today - timedelta(days=days_since_sunday)

        # Monday is 6 days before Sunday
        start_date = end_date - timedelta(days=6)

        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

    def query_revenue_summary(self, start_date: str, end_date: str) -> Dict:
        """Query total revenue, tax, tips, and average check size"""
        query = f"""
        SELECT
            COALESCE(SUM(amount), 0) as total_revenue,
            COALESCE(SUM(tax), 0) as total_tax,
            COALESCE(SUM(tip), 0) as total_tips,
            COALESCE(SUM(gratuity), 0) as total_gratuity,
            COALESCE(SUM(total), 0) as grand_total,
            COALESCE(AVG(total), 0) as avg_check_size,
            COUNT(*) as total_checks
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND (voided IS NULL OR voided = 'false')
        """
        result = list(self.bq_client.query(query).result())[0]
        return {
            "total_revenue": float(result.total_revenue or 0),
            "total_tax": float(result.total_tax or 0),
            "total_tips": float(result.total_tips or 0),
            "total_gratuity": float(result.total_gratuity or 0),
            "grand_total": float(result.grand_total or 0),
            "avg_check_size": float(result.avg_check_size or 0),
            "total_checks": int(result.total_checks or 0)
        }

    def query_order_metrics(self, start_date: str, end_date: str) -> Dict:
        """Query order counts, guest counts, and orders by dining option"""
        # Total orders and guests
        totals_query = f"""
        SELECT
            COUNT(DISTINCT order_id) as total_orders,
            COALESCE(SUM(guest_count), 0) as total_guests
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND (voided IS NULL OR voided = 'false')
        """
        totals = list(self.bq_client.query(totals_query).result())[0]

        # Orders by dining option - consolidate duplicates like "Bar, Bar" into "Bar"
        dining_query = f"""
        SELECT
            COALESCE(
                TRIM(SPLIT(dining_options, ',')[SAFE_OFFSET(0)]),
                'Unknown'
            ) as dining_option,
            COUNT(DISTINCT order_id) as order_count,
            COALESCE(SUM(total), 0) as revenue
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND (voided IS NULL OR voided = 'false')
        GROUP BY dining_option
        ORDER BY revenue DESC
        """
        dining_results = list(self.bq_client.query(dining_query).result())

        return {
            "total_orders": int(totals.total_orders or 0),
            "total_guests": int(totals.total_guests or 0),
            "by_dining_option": [
                {
                    "option": row.dining_option,
                    "orders": int(row.order_count),
                    "revenue": float(row.revenue or 0)
                }
                for row in dining_results
            ]
        }

    def query_top_items(self, start_date: str, end_date: str) -> Dict:
        """Query top 10 menu items by quantity and by revenue"""
        # Top by quantity
        qty_query = f"""
        SELECT
            menu_item,
            SUM(qty) as total_qty,
            SUM(net_price) as total_revenue
        FROM `{PROJECT_ID}.{DATASET_ID}.ItemSelectionDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND (voided IS NULL OR voided = 'false')
            AND menu_item IS NOT NULL
        GROUP BY menu_item
        ORDER BY total_qty DESC
        LIMIT 10
        """
        by_qty = list(self.bq_client.query(qty_query).result())

        # Top by revenue
        rev_query = f"""
        SELECT
            menu_item,
            SUM(qty) as total_qty,
            SUM(net_price) as total_revenue
        FROM `{PROJECT_ID}.{DATASET_ID}.ItemSelectionDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND (voided IS NULL OR voided = 'false')
            AND menu_item IS NOT NULL
        GROUP BY menu_item
        ORDER BY total_revenue DESC
        LIMIT 10
        """
        by_rev = list(self.bq_client.query(rev_query).result())

        return {
            "by_quantity": [
                {
                    "item": row.menu_item,
                    "quantity": int(row.total_qty or 0),
                    "revenue": float(row.total_revenue or 0)
                }
                for row in by_qty
            ],
            "by_revenue": [
                {
                    "item": row.menu_item,
                    "quantity": int(row.total_qty or 0),
                    "revenue": float(row.total_revenue or 0)
                }
                for row in by_rev
            ]
        }

    def query_server_performance(self, start_date: str, end_date: str) -> List[Dict]:
        """Query revenue and order count by server with gratuity split"""
        query = f"""
        SELECT
            COALESCE(server, 'Unknown') as server_name,
            COUNT(DISTINCT order_id) as order_count,
            COALESCE(SUM(total), 0) as total_revenue,
            COALESCE(SUM(tip), 0) as total_tips,
            COALESCE(SUM(gratuity), 0) as total_gratuity
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND (voided IS NULL OR voided = 'false')
        GROUP BY server
        ORDER BY total_revenue DESC
        LIMIT 15
        """
        results = list(self.bq_client.query(query).result())
        return [
            {
                "server": row.server_name,
                "orders": int(row.order_count),
                "revenue": float(row.total_revenue or 0),
                "tips": float(row.total_tips or 0),
                "gratuity": float(row.total_gratuity or 0),
                "server_grat": float(row.total_gratuity or 0) * 0.70,
                "lov3_grat": float(row.total_gratuity or 0) * 0.30
            }
            for row in results
        ]

    def query_daily_breakdown(self, start_date: str, end_date: str) -> List[Dict]:
        """Query revenue and orders per day with prior week comparison"""
        query = f"""
        WITH current_week AS (
            SELECT
                processing_date,
                FORMAT_DATE('%A', processing_date) as day_name,
                COUNT(DISTINCT order_id) as order_count,
                COALESCE(SUM(total), 0) as total_revenue,
                COALESCE(SUM(guest_count), 0) as guest_count
            FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
            WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
                AND (voided IS NULL OR voided = 'false')
            GROUP BY processing_date
        ),
        prior_week AS (
            SELECT
                DATE_ADD(processing_date, INTERVAL 7 DAY) as matching_date,
                COALESCE(SUM(total), 0) as prior_revenue
            FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
            WHERE processing_date BETWEEN DATE_SUB(DATE '{start_date}', INTERVAL 7 DAY)
                AND DATE_SUB(DATE '{end_date}', INTERVAL 7 DAY)
                AND (voided IS NULL OR voided = 'false')
            GROUP BY processing_date
        )
        SELECT
            c.processing_date,
            c.day_name,
            c.order_count,
            c.total_revenue,
            c.guest_count,
            COALESCE(p.prior_revenue, 0) as prior_revenue
        FROM current_week c
        LEFT JOIN prior_week p ON c.processing_date = p.matching_date
        ORDER BY c.processing_date
        """
        results = list(self.bq_client.query(query).result())
        daily_data = []
        for row in results:
            revenue = float(row.total_revenue or 0)
            prior = float(row.prior_revenue or 0)
            pct_change = ((revenue - prior) / prior * 100) if prior > 0 else 0
            daily_data.append({
                "date": str(row.processing_date),
                "day": row.day_name,
                "orders": int(row.order_count),
                "revenue": revenue,
                "guests": int(row.guest_count or 0),
                "prior_revenue": prior,
                "pct_change": round(pct_change, 1)
            })
        return daily_data

    def query_payment_types(self, start_date: str, end_date: str) -> List[Dict]:
        """Query payment breakdown by type"""
        query = f"""
        SELECT
            COALESCE(payment_type, 'Unknown') as payment_type,
            COUNT(*) as transaction_count,
            COALESCE(SUM(total), 0) as total_amount
        FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY payment_type
        ORDER BY total_amount DESC
        """
        results = list(self.bq_client.query(query).result())
        return [
            {
                "type": row.payment_type,
                "transactions": int(row.transaction_count),
                "amount": float(row.total_amount or 0)
            }
            for row in results
        ]

    def query_week_over_week(self, start_date: str, end_date: str) -> Dict:
        """Compare current week vs prior week and same week last year"""
        current_start = datetime.strptime(start_date, "%Y-%m-%d")
        prior_start = (current_start - timedelta(days=7)).strftime("%Y-%m-%d")
        prior_end = (current_start - timedelta(days=1)).strftime("%Y-%m-%d")
        ly_start = (current_start - timedelta(weeks=52)).strftime("%Y-%m-%d")
        ly_end = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(weeks=52)).strftime("%Y-%m-%d")

        query = f"""
        WITH current_week AS (
            SELECT
                SUM(total) as revenue,
                COUNT(DISTINCT order_id) as orders,
                SUM(guest_count) as guests,
                SUM(tip) as tips,
                AVG(total) as avg_check
            FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
            WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
                AND (voided IS NULL OR voided = 'false')
        ),
        prior_week AS (
            SELECT
                SUM(total) as revenue,
                COUNT(DISTINCT order_id) as orders,
                SUM(guest_count) as guests
            FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
            WHERE processing_date BETWEEN '{prior_start}' AND '{prior_end}'
                AND (voided IS NULL OR voided = 'false')
        ),
        last_year AS (
            SELECT
                SUM(total) as revenue,
                COUNT(DISTINCT order_id) as orders
            FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
            WHERE processing_date BETWEEN '{ly_start}' AND '{ly_end}'
                AND (voided IS NULL OR voided = 'false')
        )
        SELECT
            COALESCE(c.revenue, 0) as current_revenue,
            COALESCE(c.orders, 0) as current_orders,
            COALESCE(c.guests, 0) as current_guests,
            COALESCE(c.tips, 0) as current_tips,
            COALESCE(c.avg_check, 0) as avg_check,
            COALESCE(p.revenue, 0) as prior_revenue,
            COALESCE(p.orders, 0) as prior_orders,
            COALESCE(p.guests, 0) as prior_guests,
            COALESCE(ly.revenue, 0) as ly_revenue,
            COALESCE(ly.orders, 0) as ly_orders
        FROM current_week c, prior_week p, last_year ly
        """
        result = list(self.bq_client.query(query).result())[0]

        current_revenue = float(result.current_revenue or 0)
        prior_revenue = float(result.prior_revenue or 0)
        ly_revenue = float(result.ly_revenue or 0)

        wow_change = ((current_revenue - prior_revenue) / prior_revenue * 100) if prior_revenue > 0 else 0
        yoy_change = ((current_revenue - ly_revenue) / ly_revenue * 100) if ly_revenue > 0 else 0

        current_orders = int(result.current_orders or 0)
        prior_orders = int(result.prior_orders or 0)
        orders_change = ((current_orders - prior_orders) / prior_orders * 100) if prior_orders > 0 else 0

        return {
            "current_week": {
                "revenue": current_revenue,
                "orders": current_orders,
                "guests": int(result.current_guests or 0),
                "tips": float(result.current_tips or 0),
                "avg_check": float(result.avg_check or 0),
                "orders_per_day": round(current_orders / 7, 1)
            },
            "prior_week": {
                "revenue": prior_revenue,
                "orders": prior_orders,
                "guests": int(result.prior_guests or 0)
            },
            "last_year": {
                "revenue": ly_revenue,
                "orders": int(result.ly_orders or 0)
            },
            "changes": {
                "revenue_pct": round(wow_change, 1),
                "orders_pct": round(orders_change, 1),
                "yoy_pct": round(yoy_change, 1)
            }
        }

    def query_product_mix(self, start_date: str, end_date: str) -> Dict:
        """Query product mix by sales category"""
        query = f"""
        SELECT
            COALESCE(SUM(CASE WHEN sales_category = 'Liquor' THEN net_price ELSE 0 END), 0) as liquor_revenue,
            COALESCE(SUM(CASE WHEN sales_category = 'Food' THEN net_price ELSE 0 END), 0) as food_revenue,
            COALESCE(SUM(CASE WHEN sales_category = 'Hookah' THEN net_price ELSE 0 END), 0) as hookah_revenue,
            COALESCE(SUM(net_price), 0) as total_revenue,
            COALESCE(SUM(CASE WHEN LOWER(menu_item) LIKE '%btl%' THEN net_price ELSE 0 END), 0) as bottle_service
        FROM `{PROJECT_ID}.{DATASET_ID}.ItemSelectionDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND (voided IS NULL OR voided = 'false')
        """
        result = list(self.bq_client.query(query).result())[0]

        total = float(result.total_revenue or 1)
        liquor = float(result.liquor_revenue or 0)
        food = float(result.food_revenue or 0)
        hookah = float(result.hookah_revenue or 0)

        return {
            "liquor": {"revenue": liquor, "pct": round(liquor / total * 100, 1) if total > 0 else 0},
            "food": {"revenue": food, "pct": round(food / total * 100, 1) if total > 0 else 0},
            "hookah": {"revenue": hookah, "pct": round(hookah / total * 100, 1) if total > 0 else 0},
            "bottle_service": float(result.bottle_service or 0),
            "total": total
        }

    def query_high_check_analysis(self, start_date: str, end_date: str) -> Dict:
        """Query high-check rate (checks > $200)"""
        query = f"""
        SELECT
            COUNT(*) as total_checks,
            COUNTIF(total > 200) as high_checks
        FROM `{PROJECT_ID}.{DATASET_ID}.CheckDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        result = list(self.bq_client.query(query).result())[0]

        total = int(result.total_checks or 0)
        high = int(result.high_checks or 0)

        return {
            "total_checks": total,
            "high_checks": high,
            "high_check_rate": round(high / total * 100, 1) if total > 0 else 0,
            "target": 8.0,
            "status": "ON TARGET" if (high / total * 100 if total > 0 else 0) >= 8 else "BELOW TARGET"
        }

    def query_discount_void_control(self, start_date: str, end_date: str) -> Dict:
        """Query discount and void metrics"""
        discount_query = f"""
        SELECT
            COALESCE(SUM(discount_amount), 0) as total_discounts,
            COALESCE(SUM(amount + discount_amount), 0) as gross_plus_disc
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        disc_result = list(self.bq_client.query(discount_query).result())[0]

        void_query = f"""
        SELECT
            COUNT(DISTINCT payment_id) as total_payments,
            COUNTIF(void_date IS NOT NULL AND void_date != '') as voided_payments,
            COALESCE(SUM(CASE WHEN void_date IS NOT NULL AND void_date != '' THEN total ELSE 0 END), 0) as voided_amount
        FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        void_result = list(self.bq_client.query(void_query).result())[0]

        total_discounts = float(disc_result.total_discounts or 0)
        gross = float(disc_result.gross_plus_disc or 1)
        discount_rate = round(total_discounts / gross * 100, 1) if gross > 0 else 0

        total_payments = int(void_result.total_payments or 0)
        voided_payments = int(void_result.voided_payments or 0)
        void_rate = round(voided_payments / total_payments * 100, 2) if total_payments > 0 else 0

        return {
            "discounts": {
                "total": total_discounts,
                "gross_sales": gross,
                "rate": discount_rate,
                "benchmark": 5.0,
                "status": "OK" if discount_rate < 5 else "FLAG - HIGH DISCOUNTS"
            },
            "voids": {
                "total_payments": total_payments,
                "voided_payments": voided_payments,
                "voided_amount": float(void_result.voided_amount or 0),
                "rate": void_rate,
                "benchmark": 1.0,
                "status": "OK" if void_rate < 1 else "FLAG - HIGH VOIDS"
            }
        }

    def query_discount_breakdown(self, start_date: str, end_date: str) -> Dict:
        """Query discount breakdown by reason"""
        # Get gross sales for percentage calculation
        gross_query = f"""
        SELECT COALESCE(SUM(amount + discount_amount), 0) as gross_sales
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        gross_result = list(self.bq_client.query(gross_query).result())[0]
        gross_sales = float(gross_result.gross_sales or 1)

        query = f"""
        SELECT
            COALESCE(SUM(CASE WHEN reason_of_discount LIKE '%Manager Comp%' THEN discount ELSE 0 END), 0) as manager_comp,
            COALESCE(SUM(CASE WHEN reason_of_discount LIKE '%Open $%' OR reason_of_discount LIKE '%Open %%' THEN discount ELSE 0 END), 0) as open_discount,
            COALESCE(SUM(CASE WHEN reason_of_discount LIKE '%Owner Comp%' THEN discount ELSE 0 END), 0) as owner_comp,
            COALESCE(SUM(CASE WHEN reason_of_discount LIKE '%Birthday%' THEN discount ELSE 0 END), 0) as birthday_comp,
            COALESCE(SUM(CASE WHEN reason_of_discount LIKE '%Spillage%' OR reason_of_discount LIKE '%Quality%' THEN discount ELSE 0 END), 0) as spillage_quality
        FROM `{PROJECT_ID}.{DATASET_ID}.CheckDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND reason_of_discount IS NOT NULL
            AND reason_of_discount != ''
        """
        result = list(self.bq_client.query(query).result())[0]

        manager_comp = float(result.manager_comp or 0)
        open_discount = float(result.open_discount or 0)
        mgr_comp_pct = round(manager_comp / gross_sales * 100, 2) if gross_sales > 0 else 0

        return {
            "manager_comp": {
                "total": manager_comp,
                "pct": mgr_comp_pct,
                "threshold_pct": 4.0,
                "status": "FLAG - EXCEEDS 4%" if mgr_comp_pct > 4 else "OK"
            },
            "open_discount": {
                "total": open_discount,
                "threshold": 0,
                "status": "FLAG - SHOULD BE $0" if open_discount > 0 else "OK"
            },
            "owner_comp": float(result.owner_comp or 0),
            "birthday_comp": float(result.birthday_comp or 0),
            "spillage_quality": float(result.spillage_quality or 0)
        }

    def query_server_flags(self, start_date: str, end_date: str) -> Dict:
        """Query servers with low tip rate, high discount, or high void rate"""
        # Low tip rate (<6%)
        low_tip_query = f"""
        SELECT server, order_count, weekly_revenue, total_tips, tip_rate_pct
        FROM (
            SELECT
                server,
                COUNT(DISTINCT order_id) as order_count,
                ROUND(SUM(total), 2) as weekly_revenue,
                ROUND(SUM(tip), 2) as total_tips,
                ROUND(SUM(tip) * 100.0 / NULLIF(SUM(amount), 0), 1) as tip_rate_pct
            FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
            WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY server
            HAVING COUNT(DISTINCT order_id) >= 10
        )
        WHERE tip_rate_pct < 6
        ORDER BY tip_rate_pct
        """
        low_tip = list(self.bq_client.query(low_tip_query).result())

        # High discount rate (>15%)
        high_disc_query = f"""
        SELECT server, order_count, weekly_revenue, total_discounts, discount_rate_pct
        FROM (
            SELECT
                server,
                COUNT(DISTINCT order_id) as order_count,
                ROUND(SUM(total), 2) as weekly_revenue,
                ROUND(SUM(discount_amount), 2) as total_discounts,
                ROUND(SUM(discount_amount) * 100.0 / NULLIF(SUM(amount + discount_amount), 0), 1) as discount_rate_pct
            FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
            WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY server
            HAVING COUNT(DISTINCT order_id) >= 10
        )
        WHERE discount_rate_pct > 15
        ORDER BY discount_rate_pct DESC
        """
        high_disc = list(self.bq_client.query(high_disc_query).result())

        # High void rate (>2%)
        high_void_query = f"""
        SELECT server, total_payments, voided_payments, void_rate_pct, voided_amount
        FROM (
            SELECT
                server,
                COUNT(DISTINCT payment_id) as total_payments,
                COUNTIF(void_date IS NOT NULL AND void_date != '') as voided_payments,
                ROUND(COUNTIF(void_date IS NOT NULL AND void_date != '') * 100.0 / NULLIF(COUNT(DISTINCT payment_id), 0), 2) as void_rate_pct,
                ROUND(SUM(CASE WHEN void_date IS NOT NULL AND void_date != '' THEN total ELSE 0 END), 2) as voided_amount
            FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
            WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY server
            HAVING COUNT(DISTINCT payment_id) >= 10
        )
        WHERE void_rate_pct > 2
        ORDER BY void_rate_pct DESC
        """
        high_void = list(self.bq_client.query(high_void_query).result())

        return {
            "low_tip": [{"server": r.server, "orders": r.order_count, "revenue": float(r.weekly_revenue), "tips": float(r.total_tips), "tip_rate": float(r.tip_rate_pct)} for r in low_tip],
            "high_discount": [{"server": r.server, "orders": r.order_count, "revenue": float(r.weekly_revenue), "discounts": float(r.total_discounts), "discount_rate": float(r.discount_rate_pct)} for r in high_disc],
            "high_void": [{"server": r.server, "payments": r.total_payments, "voided": r.voided_payments, "void_rate": float(r.void_rate_pct), "voided_amount": float(r.voided_amount)} for r in high_void]
        }

    def query_cash_control(self, start_date: str, end_date: str) -> Dict:
        """Query cash control metrics"""
        cash_query = f"""
        SELECT
            COUNTIF(payment_type = 'Cash' OR payment_type LIKE '%CASH%') as cash_payments,
            COUNT(DISTINCT payment_id) as total_payments
        FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        cash_result = list(self.bq_client.query(cash_query).result())[0]

        entries_query = f"""
        SELECT
            COUNTIF(action = 'NO_SALE') as no_sale_count,
            COALESCE(SUM(CASE WHEN action = 'CLOSE_OUT_OVERAGE' THEN amount ELSE 0 END), 0) as cash_overage,
            COALESCE(SUM(CASE WHEN action = 'CLOSE_OUT_SHORTAGE' THEN amount ELSE 0 END), 0) as cash_shortage
        FROM `{PROJECT_ID}.{DATASET_ID}.CashEntries_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        entries_result = list(self.bq_client.query(entries_query).result())[0]

        cash_payments = int(cash_result.cash_payments or 0)
        total_payments = int(cash_result.total_payments or 1)
        cash_pct = round(cash_payments / total_payments * 100, 1) if total_payments > 0 else 0

        no_sale = int(entries_result.no_sale_count or 0)
        overage = float(entries_result.cash_overage or 0)
        shortage = float(entries_result.cash_shortage or 0)
        variance = abs(overage) + abs(shortage)

        return {
            "cash_pct": cash_pct,
            "cash_payments": cash_payments,
            "total_payments": total_payments,
            "cash_benchmark": 17.0,
            "cash_status": "OK" if 14 <= cash_pct <= 20 else "REVIEW",
            "no_sale_count": no_sale,
            "no_sale_threshold": 100,
            "no_sale_status": "FLAG - HIGH NO_SALE" if no_sale > 100 else "OK",
            "overage": overage,
            "shortage": shortage,
            "total_variance": variance,
            "variance_threshold": 50,
            "variance_status": "FLAG - HIGH VARIANCE" if variance > 50 else "OK"
        }

    def query_top_cash_handlers(self, start_date: str, end_date: str) -> List[Dict]:
        """Query top cash handlers"""
        query = f"""
        SELECT employee, entry_count, cash_collected, no_sale_count, payout_count
        FROM (
            SELECT
                employee,
                COUNT(*) as entry_count,
                ROUND(SUM(CASE WHEN action = 'CASH_COLLECTED' THEN amount ELSE 0 END), 2) as cash_collected,
                COUNTIF(action = 'NO_SALE') as no_sale_count,
                COUNTIF(action = 'PAY_OUT') as payout_count,
                ROW_NUMBER() OVER (ORDER BY SUM(CASE WHEN action = 'CASH_COLLECTED' THEN amount ELSE 0 END) DESC) as rank_num
            FROM `{PROJECT_ID}.{DATASET_ID}.CashEntries_raw`
            WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY employee
        )
        WHERE rank_num <= 10
        ORDER BY rank_num
        """
        results = list(self.bq_client.query(query).result())
        return [{"employee": r.employee, "entries": r.entry_count, "cash_collected": float(r.cash_collected or 0), "no_sales": r.no_sale_count, "payouts": r.payout_count} for r in results]

    def query_operational_efficiency(self, start_date: str, end_date: str) -> Dict:
        """Query kitchen fulfillment and operational metrics"""
        kitchen_query = f"""
        SELECT
            COUNT(*) as total_tickets,
            COUNTIF(fulfilled_date IS NOT NULL AND fulfilled_date != '') as fulfilled_tickets
        FROM `{PROJECT_ID}.{DATASET_ID}.KitchenTimings_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        kitchen_result = list(self.bq_client.query(kitchen_query).result())[0]

        total_tickets = int(kitchen_result.total_tickets or 0)
        fulfilled = int(kitchen_result.fulfilled_tickets or 0)
        fulfillment_rate = round(fulfilled / total_tickets * 100, 1) if total_tickets > 0 else 0

        # Station performance
        station_query = f"""
        SELECT
            station,
            COUNT(*) as ticket_count,
            COUNTIF(fulfilled_date IS NOT NULL AND fulfilled_date != '') as fulfilled_count
        FROM `{PROJECT_ID}.{DATASET_ID}.KitchenTimings_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY station
        ORDER BY ticket_count DESC
        """
        stations = list(self.bq_client.query(station_query).result())

        return {
            "total_tickets": total_tickets,
            "fulfilled_tickets": fulfilled,
            "fulfillment_rate": fulfillment_rate,
            "fulfillment_target": 99.0,
            "fulfillment_status": "OK" if fulfillment_rate >= 99 else "FLAG",
            "stations": [{"station": s.station, "tickets": s.ticket_count, "fulfilled": s.fulfilled_count, "rate": round(s.fulfilled_count / s.ticket_count * 100, 1) if s.ticket_count > 0 else 0} for s in stations]
        }

    def query_weekly_scorecard(self, start_date: str, end_date: str) -> Dict:
        """Generate weekly scorecard summary"""
        rev_query = f"""
        SELECT ROUND(SUM(total), 2) as weekly_revenue
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        rev = list(self.bq_client.query(rev_query).result())[0]

        disc_query = f"""
        SELECT ROUND(SUM(discount_amount) * 100.0 / NULLIF(SUM(amount + discount_amount), 0), 1) as discount_rate
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        disc = list(self.bq_client.query(disc_query).result())[0]

        void_query = f"""
        SELECT ROUND(COUNTIF(void_date IS NOT NULL AND void_date != '') * 100.0 / NULLIF(COUNT(*), 0), 2) as void_rate
        FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        void = list(self.bq_client.query(void_query).result())[0]

        cash_query = f"""
        SELECT COUNTIF(action = 'NO_SALE') as no_sale_count
        FROM `{PROJECT_ID}.{DATASET_ID}.CashEntries_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        cash = list(self.bq_client.query(cash_query).result())[0]

        kitchen_query = f"""
        SELECT ROUND(COUNTIF(fulfilled_date IS NOT NULL AND fulfilled_date != '') * 100.0 / NULLIF(COUNT(*), 0), 1) as fulfillment_rate
        FROM `{PROJECT_ID}.{DATASET_ID}.KitchenTimings_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        """
        kitchen = list(self.bq_client.query(kitchen_query).result())[0]

        weekly_revenue = float(rev.weekly_revenue or 0)
        discount_rate = float(disc.discount_rate or 0)
        void_rate = float(void.void_rate or 0)
        no_sale_count = int(cash.no_sale_count or 0)
        fulfillment_rate = float(kitchen.fulfillment_rate or 0)

        return {
            "revenue": {"value": weekly_revenue, "target": 100000, "status": "PASS" if weekly_revenue >= 100000 else "BELOW TARGET"},
            "discount": {"value": discount_rate, "target": 8, "status": "PASS" if discount_rate < 8 else "REVIEW"},
            "void": {"value": void_rate, "target": 1, "status": "PASS" if void_rate < 1 else "REVIEW"},
            "cash": {"value": no_sale_count, "target": 100, "status": "PASS" if no_sale_count <= 100 else "REVIEW"},
            "kitchen": {"value": fulfillment_rate, "target": 99, "status": "PASS" if fulfillment_rate >= 99 else "REVIEW"}
        }

    # ─── Business-Day-Aware Queries ─────────────────────────────────────────
    # These methods use the 4AM cutoff to assign revenue to the correct
    # business day. LOV3 is a nightlife venue: revenue at 1 AM Saturday
    # belongs to Friday's business day.

    def query_revenue_by_business_day(self, start_date: str, end_date: str) -> List[Dict]:
        """Revenue breakdown by day-of-week using the 4AM business day cutoff.

        paid_date in PaymentDetails_raw is STRING, so we CAST to DATETIME
        before applying the business-day logic.
        """
        bd = BUSINESS_DAY_SQL.format(dt_col="CAST(paid_date AS DATETIME)")
        dow = BUSINESS_DOW_SQL.format(dt_col="CAST(paid_date AS DATETIME)")
        query = f"""
        WITH business AS (
            SELECT
                {bd} AS business_date,
                {dow} AS dow_name,
                EXTRACT(DAYOFWEEK FROM {bd}) AS dow_num,
                amount, tip, gratuity, total
            FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
            WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
                AND (void_date IS NULL OR void_date = '')
                AND paid_date IS NOT NULL AND paid_date != ''
        )
        SELECT
            dow_name,
            dow_num,
            COUNT(*) AS txn_count,
            ROUND(SUM(amount), 2) AS net_sales,
            ROUND(SUM(tip), 2) AS tips,
            ROUND(SUM(gratuity), 2) AS gratuity,
            ROUND(SUM(total), 2) AS gross_revenue,
            ROUND(AVG(total), 2) AS avg_check,
            COUNT(DISTINCT business_date) AS num_days
        FROM business
        GROUP BY dow_name, dow_num
        ORDER BY dow_num
        """
        rows = list(self.bq_client.query(query).result())
        return [
            {
                "day": row.dow_name,
                "txn_count": int(row.txn_count or 0),
                "net_sales": float(row.net_sales or 0),
                "tips": float(row.tips or 0),
                "gratuity": float(row.gratuity or 0),
                "gross_revenue": float(row.gross_revenue or 0),
                "avg_check": float(row.avg_check or 0),
                "num_days": int(row.num_days or 0),
                "avg_daily_revenue": round(
                    float(row.gross_revenue or 0) / max(int(row.num_days or 1), 1), 2
                ),
            }
            for row in rows
        ]

    def query_monthly_pnl(self, start_date: str, end_date: str) -> List[Dict]:
        """Monthly P&L combining Toast revenue with bank expenses.

        Uses centralized LOV3 business assumptions for gratuity split,
        cash reconciliation, and true labor calculation.
        """
        bq = self.bq_client

        # Monthly revenue from Toast
        rev_query = f"""
        SELECT
            FORMAT_DATE('%Y-%m', processing_date) AS month,
            COALESCE(SUM(amount), 0) AS net_sales,
            COALESCE(SUM(tip), 0) AS tips,
            COALESCE(SUM(gratuity), 0) AS gratuity,
            COALESCE(SUM(total), 0) AS gross_revenue,
            COUNT(DISTINCT order_id) AS order_count
        FROM `{PROJECT_ID}.{DATASET_ID}.OrderDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
            AND (voided IS NULL OR voided = 'false')
        GROUP BY month ORDER BY month
        """
        rev_rows = {r.month: r for r in bq.query(rev_query).result()}

        # Monthly bank expenses (debits)
        # transaction_date is STRING in BankTransactions_raw, must CAST to DATE
        exp_query = f"""
        SELECT
            FORMAT_DATE('%Y-%m', CAST(transaction_date AS DATE)) AS month,
            category,
            ROUND(SUM(abs_amount), 2) AS total
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        WHERE transaction_date BETWEEN '{start_date}' AND '{end_date}'
            AND transaction_type = 'debit'
        GROUP BY month, category
        ORDER BY month, total DESC
        """
        exp_rows = list(bq.query(exp_query).result())

        # Monthly cash collected (Toast) vs deposited (bank)
        cash_toast_query = f"""
        SELECT
            FORMAT_DATE('%Y-%m', processing_date) AS month,
            COALESCE(SUM(CASE WHEN payment_type = 'Cash' OR payment_type LIKE '%CASH%'
                         THEN total ELSE 0 END), 0) AS cash_collected
        FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
        WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY month
        """
        cash_toast = {r.month: float(r.cash_collected or 0)
                      for r in bq.query(cash_toast_query).result()}

        cash_bank_query = f"""
        SELECT
            FORMAT_DATE('%Y-%m', CAST(transaction_date AS DATE)) AS month,
            COALESCE(SUM(abs_amount), 0) AS cash_deposited
        FROM `{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`
        WHERE transaction_date BETWEEN '{start_date}' AND '{end_date}'
            AND transaction_type = 'credit'
            AND (LOWER(category) LIKE '%cash deposit%'
                 OR LOWER(category) LIKE '%cash account transfer%'
                 OR LOWER(description) LIKE '%counter credit%')
        GROUP BY month
        """
        cash_bank = {r.month: float(r.cash_deposited or 0)
                     for r in bq.query(cash_bank_query).result()}

        # Build expense dict by month
        expenses_by_month: Dict[str, Dict[str, float]] = {}
        for row in exp_rows:
            m = row.month
            if m not in expenses_by_month:
                expenses_by_month[m] = {}
            expenses_by_month[m][row.category] = float(row.total or 0)

        # Helper: sum categories matching keywords
        def _sum_matching(cats: Dict[str, float], keywords: List[str]) -> float:
            return sum(v for k, v in cats.items()
                       if any(kw.lower() in k.lower() for kw in keywords))

        all_months = sorted(set(list(rev_rows.keys()) +
                                list(expenses_by_month.keys())))
        results = []
        for m in all_months:
            rev = rev_rows.get(m)
            net_sales = float(rev.net_sales or 0) if rev else 0.0
            tips = float(rev.tips or 0) if rev else 0.0
            grat = float(rev.gratuity or 0) if rev else 0.0
            gross = float(rev.gross_revenue or 0) if rev else 0.0

            grat_retained = round(grat * GRAT_RETAIN_PCT, 2)
            pass_through = round(tips + grat * GRAT_PASSTHROUGH_PCT, 2)
            adj_revenue = round(net_sales + grat_retained, 2)

            cats = expenses_by_month.get(m, {})
            total_exp = sum(v for k, v in cats.items()
                            if "revenue" not in k.lower())
            cogs = _sum_matching(cats, ["cost of goods", "cogs"])
            labor_gross = _sum_matching(cats, ["3. labor", "labor cost", "payroll"])
            labor_true = round(max(labor_gross - pass_through, 0), 2)
            marketing = _sum_matching(cats, ["marketing", "promotions",
                                              "entertainment", "event"])
            opex = _sum_matching(cats, ["operating expenses", "opex"])

            adj_expenses = round(max(total_exp - pass_through, 0), 2)
            net_profit = round(adj_revenue - adj_expenses, 2)

            cash_coll = cash_toast.get(m, 0)
            cash_dep = cash_bank.get(m, 0)
            unreconciled = round(cash_coll - cash_dep, 2)

            rev_denom = adj_revenue if adj_revenue > 0 else 1
            results.append({
                "month": m,
                "net_sales": net_sales,
                "gratuity_retained": grat_retained,
                "adjusted_revenue": adj_revenue,
                "pass_through_to_staff": pass_through,
                "cogs": cogs,
                "cogs_pct": round(cogs / rev_denom * 100, 1),
                "labor_gross": labor_gross,
                "labor_true": labor_true,
                "labor_pct": round(labor_true / rev_denom * 100, 1),
                "marketing": marketing,
                "opex": opex,
                "total_expenses_adjusted": adj_expenses,
                "net_profit": net_profit,
                "margin_pct": round(net_profit / rev_denom * 100, 1),
                "cash_collected_toast": cash_coll,
                "cash_deposited_bank": cash_dep,
                "unreconciled_cash": unreconciled,
                "order_count": int(rev.order_count or 0) if rev else 0,
            })

        return results

    def query_hourly_revenue_profile(self, start_date: str, end_date: str) -> List[Dict]:
        """Hourly revenue profile using business-day-aware grouping.

        Shows what hours generate revenue, with proper attribution of
        post-midnight hours to the prior business day.
        """
        bd = BUSINESS_DAY_SQL.format(dt_col="CAST(paid_date AS DATETIME)")
        query = f"""
        WITH parsed AS (
            SELECT
                EXTRACT(HOUR FROM CAST(paid_date AS DATETIME)) AS hour_of_day,
                {bd} AS business_date,
                amount, tip, gratuity, total
            FROM `{PROJECT_ID}.{DATASET_ID}.PaymentDetails_raw`
            WHERE processing_date BETWEEN '{start_date}' AND '{end_date}'
                AND (void_date IS NULL OR void_date = '')
                AND paid_date IS NOT NULL AND paid_date != ''
        )
        SELECT
            hour_of_day,
            COUNT(*) AS txn_count,
            ROUND(SUM(total), 2) AS gross_revenue,
            ROUND(AVG(total), 2) AS avg_check,
            COUNT(DISTINCT business_date) AS num_days
        FROM parsed
        GROUP BY hour_of_day
        ORDER BY hour_of_day
        """
        rows = list(self.bq_client.query(query).result())
        return [
            {
                "hour": int(row.hour_of_day),
                "txn_count": int(row.txn_count or 0),
                "gross_revenue": float(row.gross_revenue or 0),
                "avg_check": float(row.avg_check or 0),
                "num_days": int(row.num_days or 0),
                "avg_daily_revenue": round(
                    float(row.gross_revenue or 0) / max(int(row.num_days or 1), 1), 2
                ),
            }
            for row in rows
        ]

    def generate_insights(
        self,
        revenue: Dict,
        orders: Dict,
        top_items: Dict,
        servers: List[Dict],
        daily: List[Dict],
        payments: List[Dict],
        wow: Dict
    ) -> Tuple[List[str], List[str]]:
        """Generate key insights and recommendations from the data"""
        insights = []
        recommendations = []

        # Managers - don't suggest they share techniques or flag for tip issues
        MANAGERS = ["Tony Winn", "Sossity Taylor", "Tiffany Loving", "Ashley Baines"]
        # Bottle Manager is a pool of sales from multiple servers, not a single person
        POOL_ACCOUNTS = ["Bottle Manager"]

        def is_manager(name: str) -> bool:
            return any(mgr.lower() in name.lower() for mgr in MANAGERS)

        def is_pool_account(name: str) -> bool:
            return any(pool.lower() in name.lower() for pool in POOL_ACCOUNTS)

        # Revenue insights
        if wow["changes"]["revenue_pct"] > 10:
            insights.append(f"Strong revenue growth of {wow['changes']['revenue_pct']:.1f}% compared to last week")
        elif wow["changes"]["revenue_pct"] < -10:
            insights.append(f"Revenue declined {abs(wow['changes']['revenue_pct']):.1f}% from last week")
            recommendations.append("Review marketing efforts and consider promotions to boost traffic")

        # Average check analysis
        if revenue["total_checks"] > 0:
            avg_check = revenue["avg_check_size"]
            if avg_check > 50:
                insights.append(f"Strong average check size of ${avg_check:.2f}")
            elif avg_check < 25:
                insights.append(f"Average check size is ${avg_check:.2f}")
                recommendations.append("Train staff on upselling techniques to increase average check size")

        # Best/worst day analysis
        if daily:
            best_day = max(daily, key=lambda x: x["revenue"])
            worst_day = min(daily, key=lambda x: x["revenue"])
            insights.append(f"Best performing day: {best_day['day']} (${best_day['revenue']:,.2f})")
            insights.append(f"Slowest day: {worst_day['day']} (${worst_day['revenue']:,.2f})")
            if worst_day["revenue"] < best_day["revenue"] * 0.5:
                recommendations.append(f"Consider {worst_day['day']} specials or promotions to boost slow day sales")

        # Top performer insight
        if servers:
            top_server = servers[0]
            server_name = top_server['server']
            if is_pool_account(server_name):
                insights.append(f"Top revenue: {server_name} (pooled bottle service) with ${top_server['revenue']:,.2f} in sales")
            elif is_manager(server_name):
                insights.append(f"Top revenue: {server_name} (Manager) with ${top_server['revenue']:,.2f} in sales")
            else:
                insights.append(f"Top server: {server_name} with ${top_server['revenue']:,.2f} in sales")

            # Only suggest sharing techniques for non-managers and non-pool accounts
            if len(servers) > 1 and not is_manager(server_name) and not is_pool_account(server_name):
                avg_server_revenue = sum(s["revenue"] for s in servers) / len(servers)
                if top_server["revenue"] > avg_server_revenue * 1.5:
                    recommendations.append(f"Have {server_name} share sales techniques with the team")

        # Tip analysis
        if servers:
            total_tips = sum(s["tips"] for s in servers)
            total_server_revenue = sum(s["revenue"] for s in servers)
            if total_server_revenue > 0:
                tip_pct = (total_tips / total_server_revenue) * 100
                insights.append(f"Average tip rate: {tip_pct:.1f}%")
                if tip_pct < 15:
                    recommendations.append("Tip rate below industry average - focus on service quality")

        # Menu insights
        if top_items["by_quantity"]:
            top_item = top_items["by_quantity"][0]
            insights.append(f"Best seller: {top_item['item']} ({top_item['quantity']} sold)")

        if top_items["by_revenue"]:
            top_revenue_item = top_items["by_revenue"][0]
            if top_revenue_item["item"] != top_items["by_quantity"][0]["item"]:
                insights.append(f"Highest revenue item: {top_revenue_item['item']} (${top_revenue_item['revenue']:,.2f})")

        # Dining option insights
        if orders["by_dining_option"]:
            dine_in = next((d for d in orders["by_dining_option"] if "dine" in d["option"].lower()), None)
            takeout = next((d for d in orders["by_dining_option"] if "take" in d["option"].lower()), None)
            if dine_in and takeout:
                total_orders = orders["total_orders"]
                if total_orders > 0:
                    dine_in_pct = (dine_in["orders"] / total_orders) * 100
                    insights.append(f"Dine-in represents {dine_in_pct:.0f}% of orders")

        # Guest analysis
        if orders["total_guests"] > 0 and orders["total_orders"] > 0:
            avg_party_size = orders["total_guests"] / orders["total_orders"]
            insights.append(f"Average party size: {avg_party_size:.1f} guests")

        # Payment method insights
        if payments:
            cash_payment = next((p for p in payments if "cash" in p["type"].lower()), None)
            if cash_payment and revenue["grand_total"] > 0:
                cash_pct = (cash_payment["amount"] / revenue["grand_total"]) * 100
                insights.append(f"Cash transactions: {cash_pct:.1f}% of total")

        # General recommendations
        if orders["total_orders"] > 0 and wow["changes"]["orders_pct"] > 0:
            recommendations.append("Order volume is growing - ensure adequate staffing for peak times")

        if not recommendations:
            recommendations.append("Continue current operations - metrics are stable")

        return insights, recommendations

    def generate_html_report(
        self,
        start_date: str,
        end_date: str,
        revenue: Dict,
        orders: Dict,
        top_items: Dict,
        servers: List[Dict],
        daily: List[Dict],
        payments: List[Dict],
        wow: Dict,
        product_mix: Dict = None,
        high_check: Dict = None,
        disc_void: Dict = None,
        disc_breakdown: Dict = None,
        server_flags: Dict = None,
        cash_control: Dict = None,
        cash_handlers: List[Dict] = None,
        ops_efficiency: Dict = None,
        scorecard: Dict = None
    ) -> str:
        """Generate formatted HTML email report"""

        # Set defaults for optional params
        product_mix = product_mix or {}
        high_check = high_check or {}
        disc_void = disc_void or {"discounts": {}, "voids": {}}
        disc_breakdown = disc_breakdown or {}
        server_flags = server_flags or {"low_tip": [], "high_discount": [], "high_void": []}
        cash_control = cash_control or {}
        cash_handlers = cash_handlers or []
        ops_efficiency = ops_efficiency or {"stations": []}
        scorecard = scorecard or {}

        # Generate insights
        insights, recommendations = self.generate_insights(
            revenue, orders, top_items, servers, daily, payments, wow
        )

        # Format currency helper
        def fmt_currency(val: float) -> str:
            return f"${val:,.2f}"

        # Format percentage with arrow
        def fmt_change(val: float) -> str:
            arrow = "▲" if val > 0 else "▼" if val < 0 else "→"
            color = "#22c55e" if val > 0 else "#ef4444" if val < 0 else "#6b7280"
            return f'<span style="color: {color}">{arrow} {abs(val):.1f}%</span>'

        # Build top items by quantity table
        top_items_qty_html = ""
        for i, item in enumerate(top_items["by_quantity"][:10], 1):
            top_items_qty_html += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{i}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{item['item']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{item['quantity']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(item['revenue'])}</td>
            </tr>
            """

        # Build top items by revenue table
        top_items_rev_html = ""
        for i, item in enumerate(top_items["by_revenue"][:10], 1):
            top_items_rev_html += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{i}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{item['item']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{item['quantity']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(item['revenue'])}</td>
            </tr>
            """

        # Build server performance table with gratuity split
        servers_html = ""
        for server in servers[:10]:
            servers_html += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{server['server']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{server['orders']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(server['revenue'])}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(server['tips'])}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(server.get('gratuity', 0))}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(server.get('server_grat', 0))}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(server.get('lov3_grat', 0))}</td>
            </tr>
            """

        # Build daily breakdown table with prior week comparison
        daily_html = ""
        for day in daily:
            pct_chg = day.get('pct_change', 0)
            pct_color = '#22c55e' if pct_chg > 0 else '#ef4444' if pct_chg < 0 else '#6b7280'
            pct_arrow = '▲' if pct_chg > 0 else '▼' if pct_chg < 0 else '→'
            daily_html += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{day['day']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{day['date']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{day['orders']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{day['guests']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(day['revenue'])}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right; color: #6b7280;">{fmt_currency(day.get('prior_revenue', 0))}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right; color: {pct_color};">{pct_arrow} {abs(pct_chg):.1f}%</td>
            </tr>
            """

        # Build payment types table
        payments_html = ""
        for pmt in payments:
            payments_html += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{pmt['type']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{pmt['transactions']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(pmt['amount'])}</td>
            </tr>
            """

        # Build dining options table
        dining_html = ""
        for opt in orders["by_dining_option"]:
            dining_html += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{opt['option']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{opt['orders']}</td>
                <td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(opt['revenue'])}</td>
            </tr>
            """

        # Build insights list
        insights_html = ""
        for insight in insights:
            insights_html += f'<li style="margin-bottom: 8px; color: #374151;">{insight}</li>'

        # Build recommendations list
        recommendations_html = ""
        for rec in recommendations:
            recommendations_html += f'<li style="margin-bottom: 8px; color: #374151;">{rec}</li>'

        # Build server flags tables
        low_tip_html = ""
        for s in server_flags.get("low_tip", []):
            low_tip_html += f'<tr><td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{s["server"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{s["orders"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(s["revenue"])}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{s["tip_rate"]}%</td></tr>'

        high_disc_html = ""
        for s in server_flags.get("high_discount", []):
            high_disc_html += f'<tr><td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{s["server"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{s["orders"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(s["discounts"])}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{s["discount_rate"]}%</td></tr>'

        high_void_html = ""
        for s in server_flags.get("high_void", []):
            high_void_html += f'<tr><td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{s["server"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{s["payments"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(s["voided_amount"])}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{s["void_rate"]}%</td></tr>'

        # Build cash handlers table
        cash_handlers_html = ""
        for h in cash_handlers:
            cash_handlers_html += f'<tr><td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{h["employee"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{fmt_currency(h["cash_collected"])}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{h["no_sales"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{h["payouts"]}</td></tr>'

        # Build station performance table
        stations_html = ""
        for st in ops_efficiency.get("stations", []):
            stations_html += f'<tr><td style="padding: 8px; border-bottom: 1px solid #e5e7eb;">{st["station"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{st["tickets"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{st["fulfilled"]}</td><td style="padding: 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{st["rate"]}%</td></tr>'

        # Status badge helper
        def status_badge(status: str) -> str:
            if not status or status == 'N/A':
                return '<span style="background: #e5e7eb; color: #6b7280; padding: 4px 8px; border-radius: 4px; font-size: 12px;">N/A</span>'
            if "OK" in status or "PASS" in status or "ON TARGET" in status:
                return f'<span style="background: #dcfce7; color: #166534; padding: 4px 8px; border-radius: 4px; font-size: 12px;">{status}</span>'
            else:
                return f'<span style="background: #fee2e2; color: #991b1b; padding: 4px 8px; border-radius: 4px; font-size: 12px;">{status}</span>'

        # Pre-extract nested dict values to avoid f-string escaping issues
        sc_rev = scorecard.get('revenue') or {}
        sc_disc = scorecard.get('discount') or {}
        sc_void = scorecard.get('void') or {}
        sc_cash = scorecard.get('cash') or {}
        sc_kit = scorecard.get('kitchen') or {}

        sc_rev_val = fmt_currency(sc_rev.get('value', 0))
        sc_rev_status = status_badge(sc_rev.get('status', 'N/A'))
        sc_disc_val = f"{sc_disc.get('value', 0):.1f}"
        sc_disc_status = status_badge(sc_disc.get('status', 'N/A'))
        sc_void_val = f"{sc_void.get('value', 0):.2f}"
        sc_void_status = status_badge(sc_void.get('status', 'N/A'))
        sc_cash_val = sc_cash.get('value', 0)
        sc_cash_status = status_badge(sc_cash.get('status', 'N/A'))
        sc_kit_val = f"{sc_kit.get('value', 0):.1f}"
        sc_kit_status = status_badge(sc_kit.get('status', 'N/A'))

        pm_liq = product_mix.get('liquor') or {}
        pm_food = product_mix.get('food') or {}
        pm_hook = product_mix.get('hookah') or {}
        pm_liq_rev = fmt_currency(pm_liq.get('revenue', 0))
        pm_liq_pct = pm_liq.get('pct', 0)
        pm_food_rev = fmt_currency(pm_food.get('revenue', 0))
        pm_food_pct = pm_food.get('pct', 0)
        pm_hook_rev = fmt_currency(pm_hook.get('revenue', 0))
        pm_hook_pct = pm_hook.get('pct', 0)
        pm_bottle = fmt_currency(product_mix.get('bottle_service', 0))

        dv_disc = disc_void.get('discounts') or {}
        dv_void = disc_void.get('voids') or {}
        dv_disc_total = fmt_currency(dv_disc.get('total', 0))
        dv_disc_rate = f"{dv_disc.get('rate', 0):.1f}"
        dv_disc_status = status_badge(dv_disc.get('status', 'N/A'))
        dv_void_cnt = dv_void.get('voided_payments', 0)
        dv_void_total = dv_void.get('total_payments', 0)
        dv_void_rate = f"{dv_void.get('rate', 0):.2f}"
        dv_void_status = status_badge(dv_void.get('status', 'N/A'))
        dv_void_amt = fmt_currency(dv_void.get('voided_amount', 0))

        db_mgr = disc_breakdown.get('manager_comp') or {}
        db_open = disc_breakdown.get('open_discount') or {}
        db_mgr_total = fmt_currency(db_mgr.get('total', 0))
        db_mgr_status = status_badge(db_mgr.get('status', 'N/A'))
        db_open_total = fmt_currency(db_open.get('total', 0))
        db_open_status = status_badge(db_open.get('status', 'N/A'))
        db_owner = fmt_currency(disc_breakdown.get('owner_comp', 0))
        db_birthday = fmt_currency(disc_breakdown.get('birthday_comp', 0))
        db_spillage = fmt_currency(disc_breakdown.get('spillage_quality', 0))

        hc_total = high_check.get('total_checks', 0)
        hc_high = high_check.get('high_checks', 0)
        hc_rate = f"{high_check.get('high_check_rate', 0):.1f}"
        hc_status = status_badge(high_check.get('status', 'N/A'))

        cc_pct = f"{cash_control.get('cash_pct', 0):.1f}"
        cc_status = status_badge(cash_control.get('cash_status', 'N/A'))
        cc_nosale = cash_control.get('no_sale_count', 0)
        cc_nosale_status = status_badge(cash_control.get('no_sale_status', 'N/A'))
        cc_variance = fmt_currency(cash_control.get('total_variance', 0))
        cc_over = fmt_currency(cash_control.get('overage', 0))
        cc_short = fmt_currency(cash_control.get('shortage', 0))
        cc_var_status = status_badge(cash_control.get('variance_status', 'N/A'))

        oe_tickets = ops_efficiency.get('total_tickets', 0)
        oe_fulfilled = ops_efficiency.get('fulfilled_tickets', 0)
        oe_rate = f"{ops_efficiency.get('fulfillment_rate', 0):.1f}"
        oe_status = status_badge(ops_efficiency.get('fulfillment_status', 'N/A'))

        # Pre-compute avg party size
        avg_party_size = f"{orders['total_guests'] / orders['total_orders']:.1f}" if orders['total_orders'] > 0 else "0"

        wow_rev = fmt_currency(wow['current_week']['revenue'])
        wow_orders = wow['current_week']['orders']
        wow_guests = wow['current_week']['guests']
        wow_tips = fmt_currency(wow['current_week'].get('tips', 0))
        wow_avg = fmt_currency(wow['current_week'].get('avg_check', 0))
        wow_per_day = wow['current_week'].get('orders_per_day', 0)
        wow_rev_chg = fmt_change(wow['changes']['revenue_pct'])
        wow_ord_chg = fmt_change(wow['changes']['orders_pct'])
        wow_yoy_chg = fmt_change(wow['changes'].get('yoy_pct', 0))
        wow_prior_guests = wow['prior_week']['guests']

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; background-color: #f9fafb;">

    <div style="background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%); color: white; padding: 30px; border-radius: 12px 12px 0 0;">
        <h1 style="margin: 0 0 10px 0; font-size: 28px;">LOV3 Houston Weekly Report</h1>
        <p style="margin: 0; opacity: 0.9; font-size: 16px;">{start_date} to {end_date}</p>
    </div>

    <!-- Weekly Scorecard Summary -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Weekly Scorecard</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #f9fafb;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">Metric</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Value</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Target</th>
                    <th style="padding: 12px 8px; text-align: center; font-weight: 600; color: #374151;">Status</th>
                </tr>
            </thead>
            <tbody>
                <tr><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb;">Weekly Revenue</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{sc_rev_val}</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">$100,000</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: center;">{sc_rev_status}</td></tr>
                <tr><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb;">Discount Rate</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{sc_disc_val}%</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">&lt;8%</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: center;">{sc_disc_status}</td></tr>
                <tr><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb;">Void Rate</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{sc_void_val}%</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">&lt;1%</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: center;">{sc_void_status}</td></tr>
                <tr><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb;">NO_SALE Count</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{sc_cash_val}</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">&lt;100</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: center;">{sc_cash_status}</td></tr>
                <tr><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb;">Kitchen Fulfillment</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">{sc_kit_val}%</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right;">&gt;99%</td><td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: center;">{sc_kit_status}</td></tr>
            </tbody>
        </table>
    </div>

    <!-- Week over Week Summary -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Revenue & Volume KPIs</h2>
        <div style="display: flex; gap: 20px; flex-wrap: wrap;">
            <div style="flex: 1; min-width: 150px; background: #f0fdf4; padding: 20px; border-radius: 8px;">
                <div style="font-size: 14px; color: #166534; margin-bottom: 5px;">Revenue</div>
                <div style="font-size: 24px; font-weight: bold; color: #15803d;">{wow_rev}</div>
                <div style="font-size: 12px; margin-top: 5px;">WoW: {wow_rev_chg}</div>
                <div style="font-size: 12px;">YoY: {wow_yoy_chg}</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #eff6ff; padding: 20px; border-radius: 8px;">
                <div style="font-size: 14px; color: #1e40af; margin-bottom: 5px;">Orders</div>
                <div style="font-size: 24px; font-weight: bold; color: #1d4ed8;">{wow_orders}</div>
                <div style="font-size: 12px; margin-top: 5px;">WoW: {wow_ord_chg}</div>
                <div style="font-size: 12px;">{wow_per_day}/day</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #fef3c7; padding: 20px; border-radius: 8px;">
                <div style="font-size: 14px; color: #92400e; margin-bottom: 5px;">Avg Check</div>
                <div style="font-size: 24px; font-weight: bold; color: #b45309;">{wow_avg}</div>
                <div style="font-size: 12px; margin-top: 5px;">Target: $90</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f3e8ff; padding: 20px; border-radius: 8px;">
                <div style="font-size: 14px; color: #7c3aed; margin-bottom: 5px;">Tips</div>
                <div style="font-size: 24px; font-weight: bold; color: #6d28d9;">{wow_tips}</div>
                <div style="font-size: 12px; margin-top: 5px;">Guests: {wow_guests}</div>
            </div>
        </div>
    </div>

    <!-- Revenue Summary -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Revenue Summary</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <tr style="background: #f0fdf4;">
                <td style="padding: 12px 8px; border-bottom: 1px solid #e5e7eb; font-weight: bold; color: #166534;">Net Sales</td>
                <td style="padding: 12px 8px; border-bottom: 1px solid #e5e7eb; text-align: right; font-weight: bold; font-size: 18px; color: #15803d;">{fmt_currency(revenue['total_revenue'])}</td>
                <td style="padding: 12px 8px; border-bottom: 1px solid #e5e7eb; color: #6b7280; font-size: 12px;">Sales before gratuity, tax & tips</td>
            </tr>
            <tr>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; color: #6b7280; padding-left: 20px;">+ Auto Gratuity (20%)</td>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right; font-weight: 500;">{fmt_currency(revenue['total_gratuity'])}</td>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; color: #6b7280; font-size: 12px;">Automatic service charge</td>
            </tr>
            <tr>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; color: #6b7280; padding-left: 20px;">+ Tax Collected</td>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right; font-weight: 500;">{fmt_currency(revenue['total_tax'])}</td>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; color: #6b7280; font-size: 12px;">Sales tax</td>
            </tr>
            <tr>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; color: #6b7280; padding-left: 20px;">+ Voluntary Tips</td>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; text-align: right; font-weight: 500;">{fmt_currency(revenue['total_tips'])}</td>
                <td style="padding: 10px 8px; border-bottom: 1px solid #e5e7eb; color: #6b7280; font-size: 12px;">Additional tips beyond 20% auto gratuity</td>
            </tr>
            <tr style="background: #eff6ff;">
                <td style="padding: 12px 8px; font-weight: bold; color: #1e40af;">= Total Collected</td>
                <td style="padding: 12px 8px; text-align: right; font-weight: bold; font-size: 18px; color: #1d4ed8;">{fmt_currency(revenue['grand_total'])}</td>
                <td style="padding: 12px 8px; color: #6b7280; font-size: 12px;">Net Sales + Grat + Tax + Tips</td>
            </tr>
            <tr>
                <td style="padding: 10px 8px; color: #6b7280;">Average Check Size</td>
                <td style="padding: 10px 8px; text-align: right; font-weight: 500;">{fmt_currency(revenue['avg_check_size'])}</td>
                <td style="padding: 10px 8px; color: #6b7280; font-size: 12px;">Per order average</td>
            </tr>
        </table>
    </div>

    <!-- Order Metrics -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Order Metrics</h2>
        <div style="display: flex; gap: 20px; flex-wrap: wrap;">
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 28px; font-weight: bold; color: #1f2937;">{orders['total_orders']}</div>
                <div style="font-size: 14px; color: #6b7280;">Total Orders</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 28px; font-weight: bold; color: #1f2937;">{orders['total_guests']}</div>
                <div style="font-size: 14px; color: #6b7280;">Total Guests</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 28px; font-weight: bold; color: #1f2937;">{avg_party_size}</div>
                <div style="font-size: 14px; color: #6b7280;">Avg Party Size</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 28px; font-weight: bold; color: #1f2937;">{revenue['total_checks']}</div>
                <div style="font-size: 14px; color: #6b7280;">Total Checks</div>
            </div>
        </div>
    </div>

    <!-- Product Mix -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Product Mix</h2>
        <div style="display: flex; gap: 20px; flex-wrap: wrap; margin-bottom: 20px;">
            <div style="flex: 1; min-width: 150px; background: #fef3c7; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 24px; font-weight: bold; color: #b45309;">{pm_liq_rev}</div>
                <div style="font-size: 14px; color: #92400e;">Liquor ({pm_liq_pct}%)</div>
                <div style="font-size: 11px; color: #6b7280;">Target: 70-80%</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #dcfce7; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 24px; font-weight: bold; color: #166534;">{pm_food_rev}</div>
                <div style="font-size: 14px; color: #15803d;">Food ({pm_food_pct}%)</div>
                <div style="font-size: 11px; color: #6b7280;">Target: 15-20%</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #e0e7ff; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 24px; font-weight: bold; color: #4338ca;">{pm_hook_rev}</div>
                <div style="font-size: 14px; color: #3730a3;">Hookah ({pm_hook_pct}%)</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #fce7f3; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 24px; font-weight: bold; color: #be185d;">{pm_bottle}</div>
                <div style="font-size: 14px; color: #9d174d;">Bottle Service</div>
            </div>
        </div>
    </div>

    <!-- High-Check Analysis -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">High-Check Analysis ($200+)</h2>
        <div style="display: flex; gap: 20px; flex-wrap: wrap;">
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 28px; font-weight: bold; color: #1f2937;">{hc_total}</div>
                <div style="font-size: 14px; color: #6b7280;">Total Checks</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 28px; font-weight: bold; color: #1f2937;">{hc_high}</div>
                <div style="font-size: 14px; color: #6b7280;">High Checks ($200+)</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 28px; font-weight: bold; color: #1f2937;">{hc_rate}%</div>
                <div style="font-size: 14px; color: #6b7280;">High-Check Rate</div>
                <div style="font-size: 11px; color: #6b7280;">Target: &gt;8%</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                {hc_status}
            </div>
        </div>
    </div>

    <!-- Discount & Void Control -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Discount & Void Control</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <tr>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; color: #6b7280;">Total Discounts</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: right; font-weight: 500;">{dv_disc_total}</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: right;">Rate: {dv_disc_rate}% (Target: &lt;5%)</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: center;">{dv_disc_status}</td>
            </tr>
            <tr>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; color: #6b7280;">Voided Payments</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: right; font-weight: 500;">{dv_void_cnt} of {dv_void_total}</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: right;">Rate: {dv_void_rate}% (Target: &lt;1%)</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: center;">{dv_void_status}</td>
            </tr>
            <tr>
                <td style="padding: 10px 0; color: #6b7280;">Voided Amount</td>
                <td colspan="3" style="padding: 10px 0; text-align: right; font-weight: 500;">{dv_void_amt}</td>
            </tr>
        </table>
    </div>

    <!-- Discount Breakdown -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Discount Breakdown by Type</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <tr>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; color: #6b7280;">Manager Comp</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: right; font-weight: 500;">{db_mgr_total}</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: right;">Target: &lt;4% of Gross</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: center;">{db_mgr_status}</td>
            </tr>
            <tr>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; color: #6b7280;">Open $ Discounts</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: right; font-weight: 500;">{db_open_total}</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: right;">Target: $0</td>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: center;">{db_open_status}</td>
            </tr>
            <tr>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; color: #6b7280;">Owner Comp</td>
                <td colspan="3" style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: right; font-weight: 500;">{db_owner}</td>
            </tr>
            <tr>
                <td style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; color: #6b7280;">Birthday Comp</td>
                <td colspan="3" style="padding: 10px 0; border-bottom: 1px solid #e5e7eb; text-align: right; font-weight: 500;">{db_birthday}</td>
            </tr>
            <tr>
                <td style="padding: 10px 0; color: #6b7280;">Spillage/Quality</td>
                <td colspan="3" style="padding: 10px 0; text-align: right; font-weight: 500;">{db_spillage}</td>
            </tr>
        </table>
    </div>

    <!-- Daily Breakdown -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Daily Breakdown</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #f9fafb;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">Day</th>
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">Date</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Orders</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Guests</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Revenue</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Prior Week</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">% Change</th>
                </tr>
            </thead>
            <tbody>
                {daily_html}
            </tbody>
        </table>
    </div>

    <!-- Top Menu Items by Quantity -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Top 10 Menu Items (by Quantity)</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #f9fafb;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">#</th>
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">Item</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Qty Sold</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Revenue</th>
                </tr>
            </thead>
            <tbody>
                {top_items_qty_html}
            </tbody>
        </table>
    </div>

    <!-- Top Menu Items by Revenue -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Top 10 Menu Items (by Revenue)</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #f9fafb;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">#</th>
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">Item</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Qty Sold</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Revenue</th>
                </tr>
            </thead>
            <tbody>
                {top_items_rev_html}
            </tbody>
        </table>
    </div>

    <!-- Server Performance -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Top 10 Servers by Revenue</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #f9fafb;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">Server</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Orders</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Revenue</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Tips</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Gratuity</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Server Grat (70%)</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">LOV3 Grat (30%)</th>
                </tr>
            </thead>
            <tbody>
                {servers_html}
            </tbody>
        </table>
    </div>

    <!-- Server Flags: Low Tip Rate -->
    {"" if not server_flags.get('low_tip') else f'''
    <div style="background: #fef2f2; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #991b1b; font-size: 20px;">FLAG: Low Tip Rate (&lt;6%)</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #fee2e2;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #991b1b;">Server</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #991b1b;">Orders</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #991b1b;">Revenue</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #991b1b;">Tip Rate</th>
                </tr>
            </thead>
            <tbody>
                {low_tip_html}
            </tbody>
        </table>
    </div>
    '''}

    <!-- Server Flags: High Discount Rate -->
    {"" if not server_flags.get('high_discount') else f'''
    <div style="background: #fef2f2; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #991b1b; font-size: 20px;">FLAG: High Discount Rate (&gt;15%)</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #fee2e2;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #991b1b;">Server</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #991b1b;">Orders</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #991b1b;">Discounts</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #991b1b;">Disc Rate</th>
                </tr>
            </thead>
            <tbody>
                {high_disc_html}
            </tbody>
        </table>
    </div>
    '''}

    <!-- Server Flags: High Void Rate -->
    {"" if not server_flags.get('high_void') else f'''
    <div style="background: #fef2f2; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #991b1b; font-size: 20px;">FLAG: High Void Rate (&gt;2%)</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #fee2e2;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #991b1b;">Server</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #991b1b;">Payments</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #991b1b;">Voided Amt</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #991b1b;">Void Rate</th>
                </tr>
            </thead>
            <tbody>
                {high_void_html}
            </tbody>
        </table>
    </div>
    '''}

    <!-- Dining Options -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Orders by Dining Option</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #f9fafb;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">Dining Option</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Orders</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Revenue</th>
                </tr>
            </thead>
            <tbody>
                {dining_html}
            </tbody>
        </table>
    </div>

    <!-- Payment Types -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Payment Methods</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #f9fafb;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">Payment Type</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Transactions</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Amount</th>
                </tr>
            </thead>
            <tbody>
                {payments_html}
            </tbody>
        </table>
    </div>

    <!-- Cash Control -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Cash Control</h2>
        <div style="display: flex; gap: 20px; flex-wrap: wrap; margin-bottom: 20px;">
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 24px; font-weight: bold; color: #1f2937;">{cc_pct}%</div>
                <div style="font-size: 14px; color: #6b7280;">Cash Transactions</div>
                <div style="font-size: 11px; color: #6b7280;">Benchmark: ~17%</div>
                <div style="margin-top: 8px;">{cc_status}</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 24px; font-weight: bold; color: #1f2937;">{cc_nosale}</div>
                <div style="font-size: 14px; color: #6b7280;">NO_SALE Count</div>
                <div style="font-size: 11px; color: #6b7280;">Threshold: 100</div>
                <div style="margin-top: 8px;">{cc_nosale_status}</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 24px; font-weight: bold; color: #1f2937;">{cc_variance}</div>
                <div style="font-size: 14px; color: #6b7280;">Cash Variance</div>
                <div style="font-size: 11px; color: #6b7280;">Over: {cc_over} / Short: {cc_short}</div>
                <div style="margin-top: 8px;">{cc_var_status}</div>
            </div>
        </div>
    </div>

    <!-- Top Cash Handlers -->
    {"" if not cash_handlers else f'''
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Top Cash Handlers</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #f9fafb;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">Employee</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Cash Collected</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">NO_SALEs</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Payouts</th>
                </tr>
            </thead>
            <tbody>
                {cash_handlers_html}
            </tbody>
        </table>
    </div>
    '''}

    <!-- Operational Efficiency -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Operational Efficiency</h2>
        <div style="display: flex; gap: 20px; flex-wrap: wrap; margin-bottom: 20px;">
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 24px; font-weight: bold; color: #1f2937;">{oe_tickets}</div>
                <div style="font-size: 14px; color: #6b7280;">Kitchen Tickets</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 24px; font-weight: bold; color: #1f2937;">{oe_fulfilled}</div>
                <div style="font-size: 14px; color: #6b7280;">Fulfilled</div>
            </div>
            <div style="flex: 1; min-width: 150px; background: #f9fafb; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 24px; font-weight: bold; color: #1f2937;">{oe_rate}%</div>
                <div style="font-size: 14px; color: #6b7280;">Fulfillment Rate</div>
                <div style="font-size: 11px; color: #6b7280;">Target: 99%</div>
                <div style="margin-top: 8px;">{oe_status}</div>
            </div>
        </div>
    </div>

    <!-- Station Performance -->
    {"" if not ops_efficiency.get('stations') else f'''
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Station Performance</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #f9fafb;">
                    <th style="padding: 12px 8px; text-align: left; font-weight: 600; color: #374151;">Station</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Tickets</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Fulfilled</th>
                    <th style="padding: 12px 8px; text-align: right; font-weight: 600; color: #374151;">Rate</th>
                </tr>
            </thead>
            <tbody>
                {stations_html}
            </tbody>
        </table>
    </div>
    '''}

    <!-- Key Insights -->
    <div style="background: white; padding: 25px; border-bottom: 1px solid #e5e7eb;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Key Insights</h2>
        <ul style="margin: 0; padding-left: 20px;">
            {insights_html}
        </ul>
    </div>

    <!-- Recommendations -->
    <div style="background: white; padding: 25px; border-radius: 0 0 12px 12px;">
        <h2 style="margin: 0 0 20px 0; color: #1f2937; font-size: 20px;">Recommendations</h2>
        <ul style="margin: 0; padding-left: 20px;">
            {recommendations_html}
        </ul>
    </div>

    <!-- Footer -->
    <div style="text-align: center; padding: 20px; color: #6b7280; font-size: 12px;">
        <p>This report was automatically generated by the LOV3 Analytics Pipeline.</p>
        <p>Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} CST</p>
    </div>

</body>
</html>
        """
        return html

    def send_email(self, to_email: str, subject: str, html_content: str) -> bool:
        """Send email via SendGrid API"""
        try:
            api_key = self.secret_manager.get_secret("sendgrid-api-key")

            message = Mail(
                from_email=Email("maurice.ragland@lov3htx.com", "LOV3 Analytics"),
                to_emails=To(to_email),
                subject=subject,
                html_content=Content("text/html", html_content)
            )

            sg = SendGridAPIClient(api_key)
            response = sg.send(message)

            logger.info(f"Email sent to {to_email}, status code: {response.status_code}")
            return response.status_code in (200, 201, 202)

        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            raise

    def generate_and_send_report(self, week_ending: str = None, to_email: str = None) -> Dict:
        """Generate and send the weekly report"""
        start_date, end_date = self.get_week_dates(week_ending)

        logger.info(f"Generating weekly report for {start_date} to {end_date}")

        # Query all data
        revenue = self.query_revenue_summary(start_date, end_date)
        orders = self.query_order_metrics(start_date, end_date)
        top_items = self.query_top_items(start_date, end_date)
        servers = self.query_server_performance(start_date, end_date)
        daily = self.query_daily_breakdown(start_date, end_date)
        payments = self.query_payment_types(start_date, end_date)
        wow = self.query_week_over_week(start_date, end_date)
        product_mix = self.query_product_mix(start_date, end_date)
        high_check = self.query_high_check_analysis(start_date, end_date)
        disc_void = self.query_discount_void_control(start_date, end_date)
        disc_breakdown = self.query_discount_breakdown(start_date, end_date)
        server_flags = self.query_server_flags(start_date, end_date)
        cash_control = self.query_cash_control(start_date, end_date)
        cash_handlers = self.query_top_cash_handlers(start_date, end_date)
        ops_efficiency = self.query_operational_efficiency(start_date, end_date)
        scorecard = self.query_weekly_scorecard(start_date, end_date)

        # Generate HTML
        html = self.generate_html_report(
            start_date, end_date,
            revenue, orders, top_items, servers, daily, payments, wow,
            product_mix, high_check, disc_void, disc_breakdown, server_flags,
            cash_control, cash_handlers, ops_efficiency, scorecard
        )

        # Send email
        recipient = to_email or REPORT_EMAIL
        subject = f"LOV3 Houston Weekly Report: {start_date} to {end_date}"

        success = self.send_email(recipient, subject, html)

        return {
            "success": success,
            "week_start": start_date,
            "week_end": end_date,
            "recipient": recipient,
            "summary": {
                "total_revenue": revenue["grand_total"],
                "total_orders": orders["total_orders"],
                "total_guests": orders["total_guests"],
                "wow_revenue_change": wow["changes"]["revenue_pct"]
            }
        }

