"""Q1 2026 Leadership Financial Report generator.

Fetches all required data in a single BigQuery batch, holds it in typed
dataclasses, and renders to HTML or Markdown from the same data structure.
PDF generation lives in the standalone q1_report_pdf.py script — NOT here.
"""

from __future__ import annotations

import html
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from google.cloud import bigquery

from config import (
    Q1_2026_START, Q1_2026_END,
    Q4_2025_START, Q4_2025_END,
    Q1_2025_START, Q1_2025_END,
    Q1_2026_MONTHS,
    Q1_REPORT_FORWARD_LOOK,
    VENDOR_CONCENTRATION_THRESHOLD,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Dataclasses — one per section of the report
# ---------------------------------------------------------------------

@dataclass
class PeriodMetrics:
    """Numeric metrics for a single period (Q1 2026, Q4 2025, or Q1 2025)."""
    label: str
    gross_revenue: float = 0.0
    pos_revenue: float = 0.0
    service_charge: float = 0.0
    voluntary_tips: float = 0.0
    hookah_pos: float = 0.0
    hookah_bank: float = 0.0
    hookah_reclass: float = 0.0
    cogs: float = 0.0
    labor: float = 0.0
    opex: float = 0.0
    ebitda: float = 0.0
    covers: int = 0
    avg_check: float = 0.0
    business_days: int = 0
    labor_hours: float = 0.0


@dataclass
class RevenueSection:
    q1_2026: PeriodMetrics
    q4_2025: PeriodMetrics
    q1_2025: PeriodMetrics
    monthly: Dict[str, PeriodMetrics] = field(default_factory=dict)
    category_mix: Dict[str, float] = field(default_factory=dict)


@dataclass
class CostSection:
    q1_2026: PeriodMetrics
    q4_2025: PeriodMetrics
    q1_2025: PeriodMetrics
    opex_by_category: Dict[str, float] = field(default_factory=dict)
    labor_pct_revenue: float = 0.0


@dataclass
class ProfitabilitySection:
    q1_2026: PeriodMetrics
    q4_2025: PeriodMetrics
    q1_2025: PeriodMetrics
    ebitda_margin_q1_2026: float = 0.0
    ebitda_margin_q4_2025: float = 0.0
    ebitda_margin_q1_2025: float = 0.0


@dataclass
class KPISection:
    q1_2026: PeriodMetrics
    q4_2025: PeriodMetrics
    q1_2025: PeriodMetrics
    revenue_per_labor_hour_q1: float = 0.0
    revenue_per_business_day_q1: float = 0.0


@dataclass
class StaffPerformer:
    name: str
    attributed_revenue: float
    hours_worked: float = 0.0


@dataclass
class StaffSection:
    top_bartenders: List[StaffPerformer] = field(default_factory=list)
    top_servers: List[StaffPerformer] = field(default_factory=list)


@dataclass
class VendorSpend:
    name: str
    spend: float
    pct_of_opex: float


@dataclass
class CashFlowSection:
    total_deposits: float = 0.0
    total_expenses: float = 0.0
    by_category: Dict[str, float] = field(default_factory=dict)
    top_vendors: List[VendorSpend] = field(default_factory=list)
    concentration_warnings: List[str] = field(default_factory=list)


@dataclass
class ForwardLookSection:
    bullets: List[str] = field(default_factory=list)


@dataclass
class Q1ReportData:
    generated_at: str
    revenue: RevenueSection
    costs: CostSection
    profitability: ProfitabilitySection
    kpis: KPISection
    staff: StaffSection
    cashflow: CashFlowSection
    forward: ForwardLookSection


# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------

def compute_pct_change(current: float, prior: float) -> Optional[float]:
    """Percent change from prior to current. Returns None if prior is zero."""
    if prior == 0:
        return None
    return ((current - prior) / prior) * 100.0


def safe_div(num: float, denom: float) -> float:
    """Division that returns 0.0 when denominator is zero."""
    if denom == 0:
        return 0.0
    return num / denom


# ---------------------------------------------------------------------
# Generator (skeleton — sections filled in by later tasks)
# ---------------------------------------------------------------------

class Q1ReportGenerator:
    """Fetches Q1 report data from BigQuery and renders to HTML / Markdown."""

    def __init__(self, client: bigquery.Client):
        self.client = client

    def fetch(self) -> Q1ReportData:
        """Run all section queries and assemble Q1ReportData. Filled in Task 8."""
        raise NotImplementedError("Implemented in Task 8")

    def render_html(self, data: Q1ReportData) -> str:
        """Render full HTML page. Filled in Task 10."""
        raise NotImplementedError("Implemented in Task 10")

    def render_markdown(self, data: Q1ReportData) -> str:
        """Render markdown. Filled in Task 9."""
        raise NotImplementedError("Implemented in Task 9")
