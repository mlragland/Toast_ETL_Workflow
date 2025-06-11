# BigQuery resources for Toast ETL Pipeline

# Create BigQuery dataset
resource "google_bigquery_dataset" "toast_analytics" {
  dataset_id    = var.bigquery_dataset_id
  friendly_name = "Toast Analytics Dataset"
  description   = "Dataset for Toast POS ETL pipeline data"
  location      = var.bigquery_location
  project       = var.project_id

  # Set default table expiration (optional)
  default_table_expiration_ms = var.table_expiration_days * 24 * 60 * 60 * 1000

  labels = {
    environment = var.environment
    team        = "data-engineering"
    project     = "toast-etl"
  }

  depends_on = [google_project_service.required_apis]
}

# All Items Report Table
resource "google_bigquery_table" "all_items_report" {
  dataset_id = google_bigquery_dataset.toast_analytics.dataset_id
  table_id   = "all_items_report"
  project    = var.project_id

  description = "Toast POS - All Items Report data"

  time_partitioning {
    type  = "DAY"
    field = "loaded_at"
  }

  schema = jsonencode([
    {
      name = "GUID"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "Name"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "PLU"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "Type"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "Group"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "Price"
      type = "FLOAT64"
      mode = "NULLABLE"
    },
    {
      name = "Cost"
      type = "FLOAT64"
      mode = "NULLABLE"
    },
    {
      name = "Visibility"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "ModifiedDate"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "CreatedDate"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "loaded_at"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "source_file"
      type = "STRING"
      mode = "REQUIRED"
    }
  ])

  labels = {
    environment = var.environment
    data_source = "toast-pos"
  }
}

# Check Details Table
resource "google_bigquery_table" "check_details" {
  dataset_id = google_bigquery_dataset.toast_analytics.dataset_id
  table_id   = "check_details"
  project    = var.project_id

  description = "Toast POS - Check Details data"

  time_partitioning {
    type  = "DAY"
    field = "loaded_at"
  }

  schema = jsonencode([
    {
      name = "GUID"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "EntityType"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "ExternalId"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "CheckNumber"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "OpenedDate"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "ClosedDate"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "Server"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "Table"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "GuestCount"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "TotalAmount"
      type = "FLOAT64"
      mode = "NULLABLE"
    },
    {
      name = "TipAmount"
      type = "FLOAT64"
      mode = "NULLABLE"
    },
    {
      name = "TaxAmount"
      type = "FLOAT64"
      mode = "NULLABLE"
    },
    {
      name = "loaded_at"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "source_file"
      type = "STRING"
      mode = "REQUIRED"
    }
  ])

  labels = {
    environment = var.environment
    data_source = "toast-pos"
  }
}

# Cash Entries Table
resource "google_bigquery_table" "cash_entries" {
  dataset_id = google_bigquery_dataset.toast_analytics.dataset_id
  table_id   = "cash_entries"
  project    = var.project_id

  description = "Toast POS - Cash Entries data"

  time_partitioning {
    type  = "DAY"
    field = "loaded_at"
  }

  schema = jsonencode([
    {
      name = "GUID"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "CashDrawerGUID"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "Type"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "Amount"
      type = "FLOAT64"
      mode = "NULLABLE"
    },
    {
      name = "Date"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "Employee"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "Reason"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "loaded_at"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "source_file"
      type = "STRING"
      mode = "REQUIRED"
    }
  ])

  labels = {
    environment = var.environment
    data_source = "toast-pos"
  }
}

# Item Selection Details Table
resource "google_bigquery_table" "item_selection_details" {
  dataset_id = google_bigquery_dataset.toast_analytics.dataset_id
  table_id   = "item_selection_details"
  project    = var.project_id

  description = "Toast POS - Item Selection Details data"

  time_partitioning {
    type  = "DAY"
    field = "loaded_at"
  }

  schema = jsonencode([
    {
      name = "GUID"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "ItemGUID"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "CheckGUID"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "MenuItemGUID"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "Quantity"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "UnitPrice"
      type = "FLOAT64"
      mode = "NULLABLE"
    },
    {
      name = "TotalPrice"
      type = "FLOAT64"
      mode = "NULLABLE"
    },
    {
      name = "VoidDate"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "OrderedDate"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "loaded_at"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "source_file"
      type = "STRING"
      mode = "REQUIRED"
    }
  ])

  labels = {
    environment = var.environment
    data_source = "toast-pos"
  }
}

# Kitchen Timings Table
resource "google_bigquery_table" "kitchen_timings" {
  dataset_id = google_bigquery_dataset.toast_analytics.dataset_id
  table_id   = "kitchen_timings"
  project    = var.project_id

  description = "Toast POS - Kitchen Timings data"

  time_partitioning {
    type  = "DAY"
    field = "loaded_at"
  }

  schema = jsonencode([
    {
      name = "GUID"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "CheckGUID"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "OrderGUID"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "SentDate"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "CompletedDate"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "Station"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "loaded_at"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "source_file"
      type = "STRING"
      mode = "REQUIRED"
    }
  ])

  labels = {
    environment = var.environment
    data_source = "toast-pos"
  }
}

# Order Details Table
resource "google_bigquery_table" "order_details" {
  dataset_id = google_bigquery_dataset.toast_analytics.dataset_id
  table_id   = "order_details"
  project    = var.project_id

  description = "Toast POS - Order Details data"

  time_partitioning {
    type  = "DAY"
    field = "loaded_at"
  }

  schema = jsonencode([
    {
      name = "GUID"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "CheckGUID"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "OrderDate"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "OrderType"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "Source"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "loaded_at"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "source_file"
      type = "STRING"
      mode = "REQUIRED"
    }
  ])

  labels = {
    environment = var.environment
    data_source = "toast-pos"
  }
}

# Payment Details Table
resource "google_bigquery_table" "payment_details" {
  dataset_id = google_bigquery_dataset.toast_analytics.dataset_id
  table_id   = "payment_details"
  project    = var.project_id

  description = "Toast POS - Payment Details data"

  time_partitioning {
    type  = "DAY"
    field = "loaded_at"
  }

  schema = jsonencode([
    {
      name = "GUID"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "CheckGUID"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "PaymentType"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "Amount"
      type = "FLOAT64"
      mode = "NULLABLE"
    },
    {
      name = "TipAmount"
      type = "FLOAT64"
      mode = "NULLABLE"
    },
    {
      name = "PaymentDate"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "loaded_at"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "source_file"
      type = "STRING"
      mode = "REQUIRED"
    }
  ])

  labels = {
    environment = var.environment
    data_source = "toast-pos"
  }
} 