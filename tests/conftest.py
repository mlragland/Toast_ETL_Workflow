"""Shared test fixtures for Toast ETL Pipeline."""

import pytest
from main import app as flask_app


@pytest.fixture
def app():
    """Create Flask app configured for testing."""
    flask_app.config["TESTING"] = True
    return flask_app


@pytest.fixture
def client(app):
    """Flask test client — makes HTTP requests without a running server."""
    return app.test_client()
