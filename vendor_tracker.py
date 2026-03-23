"""
Vendor Spend Tracker — Top vendors by spend with month-over-month trends.

Surfaces cost patterns, vendor concentration, price trends, and anomalies
to help identify savings opportunities and negotiate contracts.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from google.cloud import bigquery

from config import PROJECT_ID, DATASET_ID

logger = logging.getLogger(__name__)


class VendorTracker:
    """Collects and analyzes vendor spend data from BankTransactions_raw."""

    def __init__(self):
        self.bq = bigquery.Client(project=PROJECT_ID)
        self.table = f"`{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`"

    def collect(self, start_date: str, end_date: str, limit: int = 30) -> Dict[str, Any]:
        """Collect vendor spend analysis for a date range.

        Returns: top vendors, monthly trends, category breakdown,
        concentration analysis, and anomaly flags.
        """
        data = {
            "start_date": start_date,
            "end_date": end_date,
        }

        data["top_vendors"] = self._query_top_vendors(start_date, end_date, limit)
        data["monthly_trends"] = self._query_monthly_trends(start_date, end_date)
        data["category_breakdown"] = self._query_category_breakdown(start_date, end_date)
        data["concentration"] = self._compute_concentration(data["top_vendors"])
        data["anomalies"] = self._detect_anomalies(data["monthly_trends"])
        data["kpis"] = self._compute_kpis(data)

        return data

    def _query_top_vendors(self, start_date: str, end_date: str, limit: int) -> List[Dict]:
        """Top N vendors by total spend with transaction counts and avg per txn."""
        sql = f"""
        SELECT
            vendor_normalized,
            category,
            COUNT(*) AS txn_count,
            ROUND(SUM(ABS(amount)), 2) AS total_spend,
            ROUND(AVG(ABS(amount)), 2) AS avg_per_txn,
            MIN(transaction_date) AS first_txn,
            MAX(transaction_date) AS last_txn,
            COUNT(DISTINCT FORMAT_DATE('%Y-%m', transaction_date)) AS active_months
        FROM {self.table}
        WHERE transaction_type = 'debit'
          AND transaction_date BETWEEN @start_date AND @end_date
          AND vendor_normalized IS NOT NULL
          AND vendor_normalized != ''
          AND category != 'Uncategorized'
        GROUP BY vendor_normalized, category
        ORDER BY total_spend DESC
        LIMIT @limit
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ])
        rows = list(self.bq.query(sql, job_config=cfg).result())
        return [
            {
                "vendor": r.vendor_normalized,
                "category": r.category,
                "category_section": r.category.split("/")[0] if "/" in (r.category or "") else r.category,
                "txn_count": int(r.txn_count),
                "total_spend": float(r.total_spend),
                "avg_per_txn": float(r.avg_per_txn),
                "first_txn": str(r.first_txn),
                "last_txn": str(r.last_txn),
                "active_months": int(r.active_months),
            }
            for r in rows
        ]

    def _query_monthly_trends(self, start_date: str, end_date: str) -> List[Dict]:
        """Monthly spend by top vendors for trend analysis."""
        sql = f"""
        WITH ranked AS (
            SELECT vendor_normalized, SUM(ABS(amount)) AS total
            FROM {self.table}
            WHERE transaction_type = 'debit'
              AND transaction_date BETWEEN @start_date AND @end_date
              AND vendor_normalized IS NOT NULL AND vendor_normalized != ''
              AND category != 'Uncategorized'
            GROUP BY vendor_normalized
            ORDER BY total DESC
            LIMIT 15
        )
        SELECT
            FORMAT_DATE('%Y-%m', t.transaction_date) AS month,
            t.vendor_normalized AS vendor,
            ROUND(SUM(ABS(t.amount)), 2) AS monthly_spend,
            COUNT(*) AS txn_count
        FROM {self.table} t
        JOIN ranked r ON t.vendor_normalized = r.vendor_normalized
        WHERE t.transaction_type = 'debit'
          AND t.transaction_date BETWEEN @start_date AND @end_date
        GROUP BY month, vendor
        ORDER BY month, monthly_spend DESC
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ])
        rows = list(self.bq.query(sql, job_config=cfg).result())
        return [
            {
                "month": r.month,
                "vendor": r.vendor,
                "spend": float(r.monthly_spend),
                "txn_count": int(r.txn_count),
            }
            for r in rows
        ]

    def _query_category_breakdown(self, start_date: str, end_date: str) -> List[Dict]:
        """Total spend by expense category section."""
        sql = f"""
        SELECT
            SPLIT(category, '/')[OFFSET(0)] AS section,
            ROUND(SUM(ABS(amount)), 2) AS total_spend,
            COUNT(*) AS txn_count,
            COUNT(DISTINCT vendor_normalized) AS vendor_count
        FROM {self.table}
        WHERE transaction_type = 'debit'
          AND transaction_date BETWEEN @start_date AND @end_date
          AND category != 'Uncategorized'
          AND category IS NOT NULL
        GROUP BY section
        ORDER BY total_spend DESC
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ])
        rows = list(self.bq.query(sql, job_config=cfg).result())
        return [
            {
                "section": r.section,
                "total_spend": float(r.total_spend),
                "txn_count": int(r.txn_count),
                "vendor_count": int(r.vendor_count),
            }
            for r in rows
        ]

    def _compute_concentration(self, top_vendors: List[Dict]) -> Dict:
        """Vendor concentration — top 5/10/20 share of total spend."""
        if not top_vendors:
            return {"top_5_pct": 0, "top_10_pct": 0, "top_20_pct": 0, "total_spend": 0}

        total = sum(v["total_spend"] for v in top_vendors)
        if total <= 0:
            return {"top_5_pct": 0, "top_10_pct": 0, "top_20_pct": 0, "total_spend": 0}

        top_5 = sum(v["total_spend"] for v in top_vendors[:5])
        top_10 = sum(v["total_spend"] for v in top_vendors[:10])
        top_20 = sum(v["total_spend"] for v in top_vendors[:20])

        return {
            "top_5_pct": round(top_5 / total * 100, 1),
            "top_10_pct": round(top_10 / total * 100, 1),
            "top_20_pct": round(top_20 / total * 100, 1),
            "total_spend": round(total, 2),
        }

    def _detect_anomalies(self, monthly_trends: List[Dict]) -> List[Dict]:
        """Flag vendors with >25% month-over-month spend increase."""
        # Group by vendor → list of monthly spends
        vendor_months: Dict[str, List[Dict]] = {}
        for row in monthly_trends:
            vendor_months.setdefault(row["vendor"], []).append(row)

        anomalies = []
        for vendor, months in vendor_months.items():
            months.sort(key=lambda x: x["month"])
            if len(months) < 2:
                continue

            latest = months[-1]
            prior = months[-2]
            if prior["spend"] > 0:
                change_pct = (latest["spend"] - prior["spend"]) / prior["spend"] * 100
                if change_pct > 25:
                    anomalies.append({
                        "vendor": vendor,
                        "month": latest["month"],
                        "current_spend": latest["spend"],
                        "prior_spend": prior["spend"],
                        "change_pct": round(change_pct, 1),
                        "severity": "high" if change_pct > 50 else "medium",
                    })

        anomalies.sort(key=lambda x: x["change_pct"], reverse=True)
        return anomalies

    def _compute_kpis(self, data: Dict) -> Dict:
        """Compute summary KPIs."""
        vendors = data["top_vendors"]
        categories = data["category_breakdown"]
        anomalies = data["anomalies"]

        return {
            "total_vendors": len(vendors),
            "total_spend": sum(v["total_spend"] for v in vendors),
            "total_categories": len(categories),
            "anomaly_count": len(anomalies),
            "high_anomalies": len([a for a in anomalies if a["severity"] == "high"]),
            "avg_vendor_spend": round(
                sum(v["total_spend"] for v in vendors) / len(vendors), 2
            ) if vendors else 0,
        }
