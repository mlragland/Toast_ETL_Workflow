"""
Daily Flash Report — Yesterday's key metrics at a glance.

Collects revenue, orders, guests, top servers, expenses, cash gap,
and compares to the same day last week. Formats for Slack, email, and JSON.
"""

import logging
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional

from google.cloud import bigquery

from config import (
    PROJECT_ID, DATASET_ID,
    BUSINESS_DAY_SQL, GRAT_RETAIN_PCT, GRAT_PASSTHROUGH_PCT,
    ALERT_WEBHOOK_URL, REPORT_EMAIL,
)
from services import AlertManager, SecretManager

logger = logging.getLogger(__name__)


class FlashReport:
    """Collects and formats the daily flash report."""

    def __init__(self):
        self.bq = bigquery.Client(project=PROJECT_ID)
        self.table_prefix = f"`{PROJECT_ID}.{DATASET_ID}"

    # ── Data Collection ─────────────────────────────────────────────────

    def collect(self, report_date: str = None) -> Dict[str, Any]:
        """Collect all flash report data for a given date.

        Args:
            report_date: YYYY-MM-DD string. Defaults to yesterday.

        Returns:
            Dict with keys: date, day_name, revenue, orders, guests,
            avg_check, top_servers, prior_week, expenses, cash, margins.
        """
        if not report_date:
            yesterday = date.today() - timedelta(days=1)
            report_date = yesterday.isoformat()

        dt = datetime.strptime(report_date, "%Y-%m-%d")
        day_name = dt.strftime("%A")
        prior_date = (dt - timedelta(days=7)).strftime("%Y-%m-%d")

        data = {
            "date": report_date,
            "day_name": day_name,
            "display_date": dt.strftime("%b %d, %Y"),
        }

        # Run all queries
        data.update(self._query_revenue(report_date))
        data["top_servers"] = self._query_top_servers(report_date)
        data["prior_week"] = self._query_revenue(prior_date)
        data["expenses"] = self._query_expenses(report_date)
        data["cash"] = self._query_cash(report_date)
        data["margins"] = self._compute_margins(data)

        return data

    def _query_revenue(self, d: str) -> Dict:
        """Revenue, orders, guests, avg check for a single date."""
        sql = f"""
        SELECT
            COALESCE(SUM(total), 0) AS revenue,
            COUNT(DISTINCT order_id) AS orders,
            COALESCE(SUM(guest_count), 0) AS guests,
            COALESCE(AVG(total), 0) AS avg_check,
            COALESCE(SUM(tip), 0) AS tips,
            COALESCE(SUM(gratuity), 0) AS gratuity
        FROM {self.table_prefix}.OrderDetails_raw`
        WHERE processing_date = @d
          AND (voided IS NULL OR voided = 'false')
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("d", "STRING", d),
        ])
        row = list(self.bq.query(sql, job_config=cfg).result())[0]
        return {
            "revenue": round(float(row.revenue), 2),
            "orders": int(row.orders),
            "guests": int(row.guests),
            "avg_check": round(float(row.avg_check), 2),
            "tips": round(float(row.tips), 2),
            "gratuity": round(float(row.gratuity), 2),
        }

    def _query_top_servers(self, d: str) -> List[Dict]:
        """Top 5 servers by revenue for a single date."""
        sql = f"""
        SELECT
            COALESCE(server, 'Unknown') AS server_name,
            COUNT(DISTINCT order_id) AS order_count,
            COALESCE(SUM(total), 0) AS total_revenue,
            COALESCE(SUM(tip), 0) AS tips,
            COALESCE(SUM(gratuity), 0) AS gratuity
        FROM {self.table_prefix}.OrderDetails_raw`
        WHERE processing_date = @d
          AND (voided IS NULL OR voided = 'false')
        GROUP BY server
        ORDER BY total_revenue DESC
        LIMIT 5
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("d", "STRING", d),
        ])
        rows = list(self.bq.query(sql, job_config=cfg).result())
        return [
            {
                "server": r.server_name,
                "revenue": round(float(r.total_revenue), 2),
                "orders": int(r.order_count),
                "tips": round(float(r.tips), 2),
            }
            for r in rows
        ]

    def _query_expenses(self, d: str) -> Dict:
        """Expense totals by major category section for a single date."""
        sql = f"""
        SELECT
            CASE
                WHEN LOWER(category) LIKE '%cost of goods%' OR LOWER(category) LIKE '%cogs%'
                    THEN 'cogs'
                WHEN LOWER(category) LIKE '%labor%' OR LOWER(category) LIKE '%payroll%'
                    THEN 'labor'
                WHEN LOWER(category) LIKE '%marketing%' OR LOWER(category) LIKE '%entertainment%'
                    OR LOWER(category) LIKE '%promoter%'
                    THEN 'marketing'
                WHEN LOWER(category) LIKE '%operating%' OR LOWER(category) LIKE '%opex%'
                    THEN 'opex'
                ELSE 'other'
            END AS section,
            SUM(ABS(amount)) AS total
        FROM {self.table_prefix}.BankTransactions_raw`
        WHERE transaction_date = @d
          AND transaction_type = 'debit'
          AND category != 'Uncategorized'
        GROUP BY section
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("d", "STRING", d),
        ])
        rows = list(self.bq.query(sql, job_config=cfg).result())
        expenses = {r.section: round(float(r.total), 2) for r in rows}
        expenses.setdefault("cogs", 0)
        expenses.setdefault("labor", 0)
        expenses.setdefault("marketing", 0)
        expenses.setdefault("opex", 0)
        return expenses

    def _query_cash(self, d: str) -> Dict:
        """Cash collected (POS) vs deposited (bank) for a single date."""
        bd = BUSINESS_DAY_SQL.format(dt_col="CAST(paid_date AS DATETIME)")

        # POS cash collected
        pos_sql = f"""
        SELECT COALESCE(SUM(total), 0) AS cash_collected
        FROM {self.table_prefix}.PaymentDetails_raw`
        WHERE {bd} = @d
          AND LOWER(COALESCE(payment_type, '')) = 'cash'
          AND status = 'CAPTURED'
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("d", "STRING", d),
        ])
        pos_row = list(self.bq.query(pos_sql, job_config=cfg).result())[0]
        collected = round(float(pos_row.cash_collected), 2)

        # Bank cash deposits (counter credits)
        bank_sql = f"""
        SELECT COALESCE(SUM(amount), 0) AS cash_deposited
        FROM {self.table_prefix}.BankTransactions_raw`
        WHERE transaction_date = @d
          AND amount > 0
          AND (LOWER(description) LIKE '%counter credit%'
               OR LOWER(description) LIKE '%cash deposit%')
        """
        bank_row = list(self.bq.query(bank_sql, job_config=cfg).result())[0]
        deposited = round(float(bank_row.cash_deposited), 2)

        return {
            "collected": collected,
            "deposited": deposited,
            "gap": round(collected - deposited, 2),
        }

    def _compute_margins(self, data: Dict) -> Dict:
        """Compute COGS %, labor %, net margin % from collected data."""
        revenue = data.get("revenue", 0)
        if revenue <= 0:
            return {"cogs_pct": 0, "labor_pct": 0, "net_pct": 0}

        # Adjust revenue: total - tip/grat pass-through
        grat = data.get("gratuity", 0)
        tips = data.get("tips", 0)
        pass_through = tips + grat * GRAT_PASSTHROUGH_PCT
        adj_revenue = revenue + grat * GRAT_RETAIN_PCT

        expenses = data.get("expenses", {})
        cogs = expenses.get("cogs", 0)
        labor_gross = expenses.get("labor", 0)
        true_labor = max(labor_gross - pass_through, 0)
        total_expenses = sum(expenses.values())

        return {
            "cogs_pct": round(cogs / adj_revenue * 100, 1) if adj_revenue else 0,
            "labor_pct": round(true_labor / adj_revenue * 100, 1) if adj_revenue else 0,
            "net_pct": round((adj_revenue - total_expenses) / adj_revenue * 100, 1) if adj_revenue else 0,
            "adj_revenue": round(adj_revenue, 2),
        }

    # ── Formatting ──────────────────────────────────────────────────────

    def format_slack(self, data: Dict) -> str:
        """Format flash report for Slack message."""
        rev = data["revenue"]
        prior_rev = data["prior_week"].get("revenue", 0)
        pct_change = ((rev - prior_rev) / prior_rev * 100) if prior_rev else 0
        arrow = "↑" if pct_change >= 0 else "↓"

        top = data["top_servers"]
        top_str = ""
        if top:
            top_str = f"\n\n🏆 *Top Server:* {top[0]['server']} (${top[0]['revenue']:,.0f})"
            if len(top) > 1:
                runners = " | ".join(f"#{i+2} {s['server']} (${s['revenue']:,.0f})" for i, s in enumerate(top[1:3]))
                top_str += f"\n   {runners}"

        m = data["margins"]
        cogs_icon = "✅" if m["cogs_pct"] <= 30 else "⚠️" if m["cogs_pct"] <= 35 else "🔴"
        labor_icon = "✅" if m["labor_pct"] <= 28 else "⚠️" if m["labor_pct"] <= 33 else "🔴"
        net_icon = "✅" if m["net_pct"] >= 12 else "⚠️" if m["net_pct"] >= 5 else "🔴"

        cash = data["cash"]
        gap_icon = "✅" if abs(cash["gap"]) < 100 else "⚠️" if abs(cash["gap"]) < 500 else "🔴"

        return (
            f"🍴 *LOV3 Daily Flash — {data['day_name']} {data['display_date']}*\n\n"
            f"💰 *Revenue:* ${rev:,.0f} ({arrow}{abs(pct_change):.0f}% vs last {data['day_name'][:3]})\n"
            f"📋 *Orders:* {data['orders']} | 👥 *Guests:* {data['guests']}\n"
            f"💳 *Avg Check:* ${data['avg_check']:,.2f}"
            f"{top_str}\n\n"
            f"📊 *Margins*\n"
            f"   COGS: {m['cogs_pct']}% {cogs_icon} | Labor: {m['labor_pct']}% {labor_icon} | Net: {m['net_pct']}% {net_icon}\n\n"
            f"💵 *Cash:* ${cash['collected']:,.0f} collected | ${cash['deposited']:,.0f} deposited | Gap: ${cash['gap']:,.0f} {gap_icon}"
        )

    def format_json(self, data: Dict) -> Dict:
        """Format flash report for API JSON response."""
        prior = data["prior_week"]
        rev = data["revenue"]
        prior_rev = prior.get("revenue", 0)

        return {
            "date": data["date"],
            "day_name": data["day_name"],
            "revenue": rev,
            "orders": data["orders"],
            "guests": data["guests"],
            "avg_check": data["avg_check"],
            "tips": data["tips"],
            "gratuity": data["gratuity"],
            "top_servers": data["top_servers"],
            "comparison": {
                "prior_date": (datetime.strptime(data["date"], "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d"),
                "prior_revenue": prior_rev,
                "prior_orders": prior.get("orders", 0),
                "prior_guests": prior.get("guests", 0),
                "revenue_change_pct": round((rev - prior_rev) / prior_rev * 100, 1) if prior_rev else 0,
            },
            "expenses": data["expenses"],
            "margins": data["margins"],
            "cash": data["cash"],
        }

    # ── Delivery ────────────────────────────────────────────────────────

    def send_slack(self, data: Dict):
        """Send flash report to Slack."""
        alert = AlertManager(slack_webhook=ALERT_WEBHOOK_URL)
        msg = self.format_slack(data)
        alert.send_slack_alert(msg, is_error=False)
        logger.info(f"Flash report sent to Slack for {data['date']}")

    def send_email(self, data: Dict, to_email: str = None):
        """Send flash report via email."""
        try:
            sm = SecretManager(PROJECT_ID)
            api_key = sm.get_secret("sendgrid-api-key")

            from sendgrid import SendGridAPIClient
            from sendgrid.helpers.mail import Mail, Email, To, Content

            to = to_email or REPORT_EMAIL
            subject = f"LOV3 Daily Flash — {data['day_name']} {data['display_date']}"

            # Reuse Slack format as plain text fallback, wrap in simple HTML
            slack_text = self.format_slack(data)
            html = f"<pre style='font-family:monospace;font-size:14px;line-height:1.6'>{slack_text}</pre>"

            message = Mail(
                from_email=Email("maurice.ragland@lov3htx.com", "LOV3 Analytics"),
                to_emails=To(to),
                subject=subject,
                html_content=Content("text/html", html),
            )
            sg = SendGridAPIClient(api_key)
            sg.send(message)
            logger.info(f"Flash report emailed to {to} for {data['date']}")
        except Exception as e:
            logger.error(f"Flash report email failed: {e}")
