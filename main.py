"""Toast ETL Pipeline — Flask application entry point."""

import os
import json
import logging
import time
import uuid

from flask import Flask, request, g

from routes_etl import bp as etl_bp
from routes_bank import bp as bank_bp
from routes_dashboards import bp as dashboards_bp
from routes_analytics import bp as analytics_bp


# ─── Structured JSON logging for Cloud Run ─────────────────────────────────
class StructuredFormatter(logging.Formatter):
    """JSON log formatter compatible with Cloud Logging.

    Cloud Run automatically parses JSON logs and extracts severity,
    message, and custom fields into structured log entries.
    """

    def format(self, record):
        entry = {
            "severity": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
        }
        # Attach request_id if available
        if hasattr(record, "request_id"):
            entry["request_id"] = record.request_id
        # Cloud Run trace header correlation
        if hasattr(record, "trace"):
            entry["logging.googleapis.com/trace"] = record.trace
        # Include exception info
        if record.exc_info and record.exc_info[0]:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)


def _setup_logging():
    """Configure structured logging — JSON in production, plain in dev."""
    handler = logging.StreamHandler()
    if os.environ.get("PORT"):  # Cloud Run sets PORT
        handler.setFormatter(StructuredFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s [%(request_id)s] %(module)s: %(message)s",
            defaults={"request_id": "-"},
        ))
    logging.root.handlers = [handler]
    logging.root.setLevel(logging.INFO)


_setup_logging()


# ─── Flask app ──────────────────────────────────────────────────────────────
app = Flask(__name__)
app.register_blueprint(etl_bp)
app.register_blueprint(bank_bp)
app.register_blueprint(dashboards_bp)
app.register_blueprint(analytics_bp)


@app.before_request
def _attach_request_context():
    """Generate request ID and start timer for every request."""
    # Use Cloud Run trace header if available, otherwise generate UUID
    trace_header = request.headers.get("X-Cloud-Trace-Context", "")
    g.request_id = trace_header.split("/")[0] if trace_header else uuid.uuid4().hex[:12]
    g.start_time = time.time()


@app.after_request
def _log_request(response):
    """Log every request with timing and request ID."""
    duration_ms = round((time.time() - g.get("start_time", time.time())) * 1000)
    request_id = g.get("request_id", "-")

    # Skip noisy dashboard HTML requests from access log (still logged on error)
    if response.status_code < 400 and request.path in (
        "/bank-review", "/pnl", "/analysis", "/cash-recon", "/menu-mix",
        "/events", "/loyalty", "/servers", "/kitchen", "/labor",
        "/menu-eng", "/kpi-benchmarks", "/budget", "/event-roi",
    ):
        return response

    logger = logging.getLogger("access")
    logger.info(
        "%s %s %s %dms",
        request.method, request.path, response.status_code, duration_ms,
        extra={"request_id": request_id},
    )
    return response


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=True)
