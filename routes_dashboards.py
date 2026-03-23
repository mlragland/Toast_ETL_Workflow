"""Dashboard GET routes: thin wrappers returning pre-built HTML pages."""

from flask import Blueprint, Response

from dashboards import (
    _bank_review_html, _pnl_dashboard_html, _analysis_dashboard_html,
    _cash_recon_html, _menu_mix_html, _events_calendar_html,
    _customer_loyalty_html, _server_performance_html, _kitchen_speed_html,
    _labor_dashboard_html, _menu_engineering_html, _kpi_benchmarks_html,
    _budget_html, _event_roi_html, _flash_report_html,
)

bp = Blueprint("dashboards", __name__)


@bp.route("/bank-review", methods=["GET"])
def bank_review():
    """Interactive HTML dashboard for reviewing and categorizing bank transactions."""
    return Response(_bank_review_html(), mimetype="text/html")


@bp.route("/pnl", methods=["GET"])
def pnl_dashboard():
    """Interactive P&L summary dashboard."""
    return Response(_pnl_dashboard_html(), mimetype="text/html")


@bp.route("/analysis", methods=["GET"])
def analysis_dashboard():
    """Interactive comprehensive analysis dashboard."""
    return Response(_analysis_dashboard_html(), mimetype="text/html")


@bp.route("/cash-recon", methods=["GET"])
def cash_recon_dashboard():
    """Interactive cash reconciliation dashboard."""
    return Response(_cash_recon_html(), mimetype="text/html")


@bp.route("/menu-mix", methods=["GET"])
def menu_mix_dashboard():
    """Interactive menu mix / item analysis dashboard."""
    return Response(_menu_mix_html(), mimetype="text/html")


@bp.route("/events")
def events_page():
    """Events & Promotional Calendar dashboard."""
    return Response(_events_calendar_html(), mimetype="text/html")


@bp.route("/loyalty", methods=["GET"])
def loyalty_page():
    """Guest Intelligence dashboard — card-based segmentation & analytics."""
    return Response(_customer_loyalty_html(), mimetype="text/html")


@bp.route("/servers", methods=["GET"])
def servers_page():
    return _server_performance_html()


@bp.route("/kitchen", methods=["GET"])
def kitchen_page():
    return _kitchen_speed_html()


@bp.route("/labor", methods=["GET"])
def labor_page():
    return _labor_dashboard_html()


@bp.route("/menu-eng", methods=["GET"])
def menu_eng_page():
    return _menu_engineering_html()


@bp.route("/kpi-benchmarks", methods=["GET"])
def kpi_benchmarks_page():
    return Response(_kpi_benchmarks_html(), mimetype="text/html")


@bp.route("/budget", methods=["GET"])
def budget_page():
    return Response(_budget_html(), mimetype="text/html")


@bp.route("/event-roi", methods=["GET"])
def event_roi_page():
    return Response(_event_roi_html(), mimetype="text/html")


@bp.route("/flash", methods=["GET"])
def flash_report_page():
    """Daily Flash Report dashboard."""
    return Response(_flash_report_html(), mimetype="text/html")
