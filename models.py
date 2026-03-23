from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List


@dataclass
class PipelineResult:
    """Result of a single file processing"""
    filename: str
    status: str  # success, error, skipped
    rows_processed: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    error_message: str = ""
    schema_changes: List[str] = None

    def __post_init__(self):
        if self.schema_changes is None:
            self.schema_changes = []


@dataclass
class PipelineRunSummary:
    """Summary of entire pipeline run"""
    run_id: str
    processing_date: str
    start_time: datetime
    end_time: datetime = None
    status: str = "running"
    files_processed: int = 0
    files_failed: int = 0
    total_rows: int = 0
    results: List[PipelineResult] = None
    errors: List[str] = None

    def __post_init__(self):
        if self.results is None:
            self.results = []
        if self.errors is None:
            self.errors = []


@dataclass
class BankUploadResult:
    """Result of a bank CSV upload"""
    batch_id: str
    filename: str
    status: str  # success, error
    rows_loaded: int = 0
    transactions_by_category: Dict[str, float] = None
    total_debits: float = 0.0
    total_credits: float = 0.0
    date_range: str = ""
    error_message: str = ""

    def __post_init__(self):
        if self.transactions_by_category is None:
            self.transactions_by_category = {}
