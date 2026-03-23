"""Toast ETL Pipeline — Flask application entry point."""

import os
import logging

from flask import Flask

from routes_etl import bp as etl_bp
from routes_bank import bp as bank_bp
from routes_dashboards import bp as dashboards_bp
from routes_analytics import bp as analytics_bp

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
app.register_blueprint(etl_bp)
app.register_blueprint(bank_bp)
app.register_blueprint(dashboards_bp)
app.register_blueprint(analytics_bp)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=True)
