"""Bank transaction routes: CSV upload, category management, transaction API."""

import json
import hashlib
import logging
from dataclasses import asdict
from datetime import datetime
from typing import Dict, List

from flask import Blueprint, request, jsonify
from google.cloud import bigquery

from config import PROJECT_ID, DATASET_ID
from models import BankUploadResult
from services import BofACSVParser, BankCategoryManager, CheckRegisterSync, BigQueryLoader

logger = logging.getLogger(__name__)

bp = Blueprint("bank", __name__)


@bp.route("/upload-bank-csv", methods=["POST"])
def upload_bank_csv():
    """
    Upload a Bank of America CSV export.

    Accepts multipart file upload. Auto-categorizes transactions and loads
    to BigQuery (idempotent by batch_id derived from file hash).

    Returns summary with category breakdown.
    """
    if "file" not in request.files:
        return jsonify({"error": "No file provided. Use multipart form with key 'file'."}), 400

    uploaded = request.files["file"]
    if uploaded.filename == "":
        return jsonify({"error": "Empty filename"}), 400

    try:
        file_content = uploaded.read()
        source_filename = uploaded.filename or "unknown.csv"

        # Deterministic batch_id from file content for idempotent re-uploads
        batch_id = hashlib.sha256(file_content).hexdigest()[:16]

        bq_client = bigquery.Client(project=PROJECT_ID)
        loader = BigQueryLoader(bq_client, DATASET_ID)
        cat_manager = BankCategoryManager(bq_client, DATASET_ID)

        # Get rules from BigQuery (seeds defaults on first call)
        rules = cat_manager.list_rules()

        # Sync check register from Google Sheet for "Check XXXX" lookups
        check_register: Dict[str, Dict] = {}
        try:
            register = CheckRegisterSync(bq_client, DATASET_ID)
            register.sync_from_sheet()
            check_register = register.get_lookup()
        except Exception as reg_err:
            logger.warning(f"Check register sync skipped: {reg_err}")

        parser = BofACSVParser(rules, check_register=check_register)

        df = parser.parse(file_content, source_filename)

        if df.empty:
            return jsonify({"error": "No transactions found in CSV"}), 400

        # Add upload metadata
        df["upload_date"] = datetime.now().strftime("%Y-%m-%d")
        df["upload_batch_id"] = batch_id

        # Build result summary before loading
        result = BankUploadResult(
            batch_id=batch_id,
            filename=source_filename,
            status="success",
            rows_loaded=len(df),
            total_debits=float(df.loc[df["transaction_type"] == "debit", "abs_amount"].sum()),
            total_credits=float(df.loc[df["transaction_type"] == "credit", "abs_amount"].sum()),
        )

        # Category breakdown
        cat_summary = (
            df.groupby("category")["abs_amount"]
            .sum()
            .sort_values(ascending=False)
            .to_dict()
        )
        result.transactions_by_category = {k: round(v, 2) for k, v in cat_summary.items()}

        # Date range
        min_date = df["transaction_date"].min()
        max_date = df["transaction_date"].max()
        result.date_range = f"{min_date} to {max_date}"

        # MERGE-based dedup: upsert on (transaction_date, description, amount)
        # so overlapping CSV uploads never create duplicate rows
        table_name = "BankTransactions_raw"
        if loader.table_exists(table_name):
            # Load into a temp table, then MERGE into the target
            temp_table = f"{table_name}_staging_{batch_id[:8]}"
            loader.create_table_from_df(df, temp_table)

            target_ref = loader.get_table_ref(table_name)
            temp_ref = loader.get_table_ref(temp_table)

            # Dedup key: (transaction_date, description, amount)
            # On match: only update rows that haven't been manually categorized
            # so dashboard edits are preserved across re-uploads
            key_cols = ["transaction_date", "description", "amount"]
            # Metadata columns safe to always refresh
            metadata_cols = ["upload_date", "upload_batch_id", "source_file",
                             "running_balance"]
            # Category columns only refreshed if not manually set
            category_cols = ["category", "category_source", "vendor_normalized"]
            non_key_cols = [c for c in df.columns if c not in key_cols
                           and c not in metadata_cols and c not in category_cols]

            join_cond = " AND ".join(
                [f"T.{k} = S.{k}" for k in key_cols]
            )

            # Always update metadata
            update_parts = [f"T.{c} = S.{c}" for c in metadata_cols if c in df.columns]
            # Update remaining non-key/non-category cols
            update_parts += [f"T.{c} = S.{c}" for c in non_key_cols]
            # Only overwrite category fields if NOT manually categorized
            for c in category_cols:
                if c in df.columns:
                    update_parts.append(
                        f"T.{c} = IF(T.category_source = 'manual', T.{c}, S.{c})"
                    )
            update_set = ", ".join(update_parts)

            all_cols = ", ".join(df.columns)
            src_cols = ", ".join([f"S.{c}" for c in df.columns])

            merge_sql = f"""
            MERGE `{target_ref}` T
            USING `{temp_ref}` S
            ON {join_cond}
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({all_cols})
                VALUES ({src_cols})
            """
            merge_job = bq_client.query(merge_sql)
            merge_job.result()
            rows_merged = merge_job.num_dml_affected_rows or len(df)

            # Clean up staging table
            bq_client.delete_table(temp_ref, not_found_ok=True)

            logger.info(f"Bank MERGE complete: {rows_merged} rows affected")
        else:
            loader.create_table_from_df(df, table_name)

        logger.info(
            f"Bank CSV uploaded: {source_filename}, batch={batch_id}, "
            f"rows={len(df)}, debits=${result.total_debits:,.2f}, "
            f"credits=${result.total_credits:,.2f}"
        )

        return jsonify(asdict(result))

    except ValueError as e:
        logger.error(f"Bank CSV parse error: {e}")
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"Bank CSV upload failed: {e}")
        return jsonify({"error": f"Upload failed: {str(e)}"}), 500


@bp.route("/bank-categories", methods=["GET", "POST"])
def bank_categories():
    """
    Manage bank transaction auto-categorization rules.

    GET: List all keyword -> category rules.
    POST: Add/update/delete rules.
      Body for upsert: {"action": "upsert", "keyword": "SYSCO", "category": "COGS/Food", "vendor_normalized": "Sysco"}
      Body for delete: {"action": "delete", "keyword": "SYSCO"}
    """
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        manager = BankCategoryManager(bq_client, DATASET_ID)

        if request.method == "GET":
            rules = manager.list_rules()
            return jsonify({"rules": rules, "count": len(rules)})

        # POST
        data = request.get_json()
        if not data:
            return jsonify({"error": "JSON body required"}), 400

        action = data.get("action", "upsert")

        if action == "delete":
            keyword = data.get("keyword")
            if not keyword:
                return jsonify({"error": "keyword required for delete"}), 400
            manager.delete_rule(keyword)
            return jsonify({"status": "deleted", "keyword": keyword})

        # upsert
        keyword = data.get("keyword")
        category = data.get("category")
        if not keyword or not category:
            return jsonify({"error": "keyword and category required"}), 400
        vendor = data.get("vendor_normalized", "")
        manager.upsert_rule(keyword, category, vendor)
        return jsonify({
            "status": "upserted",
            "keyword": keyword,
            "category": category,
            "vendor_normalized": vendor,
        })

    except Exception as e:
        logger.error(f"Bank categories error: {e}")
        return jsonify({"error": str(e)}), 500


# ─── Bank Transaction Review Dashboard ──────────────────────────────────────


@bp.route("/api/bank-transactions", methods=["GET"])
def api_bank_transactions():
    """
    Paginated bank transaction API with filtering.

    Query params:
        status: uncategorized | categorized | all (default: all)
        limit: max rows (default 50, max 500)
        offset: pagination offset (default 0)
        sort: date_desc | date_asc | amount_desc | amount_asc (default: date_desc)
        search: free-text search on description
        date_from: YYYY-MM-DD
        date_to: YYYY-MM-DD

    Returns: summary stats, distinct categories, paginated transaction rows.
    """
    try:
        status = request.args.get("status", "all")
        limit = min(int(request.args.get("limit", 50)), 500)
        offset = int(request.args.get("offset", 0))
        sort = request.args.get("sort", "date_desc")
        search = request.args.get("search", "").strip()
        date_from = request.args.get("date_from", "")
        date_to = request.args.get("date_to", "")

        bq_client = bigquery.Client(project=PROJECT_ID)
        table = f"`{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`"

        # Build WHERE clauses
        where_parts: List[str] = []
        params: List[bigquery.ScalarQueryParameter] = []

        if status == "uncategorized":
            where_parts.append("(category_source = 'uncategorized' OR category = 'Uncategorized')")
        elif status == "categorized":
            where_parts.append("category_source != 'uncategorized' AND category != 'Uncategorized'")

        if search:
            where_parts.append("UPPER(description) LIKE CONCAT('%', UPPER(@search), '%')")
            params.append(bigquery.ScalarQueryParameter("search", "STRING", search))

        if date_from:
            where_parts.append("transaction_date >= @date_from")
            params.append(bigquery.ScalarQueryParameter("date_from", "STRING", date_from))

        if date_to:
            where_parts.append("transaction_date <= @date_to")
            params.append(bigquery.ScalarQueryParameter("date_to", "STRING", date_to))

        where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

        # Sort mapping
        sort_map = {
            "date_desc": "transaction_date DESC, description",
            "date_asc": "transaction_date ASC, description",
            "amount_desc": "ABS(amount) DESC",
            "amount_asc": "ABS(amount) ASC",
        }
        order_sql = sort_map.get(sort, "transaction_date DESC, description")

        # Summary stats (unfiltered totals for KPI cards)
        summary_sql = f"""
        SELECT
            COUNTIF(category_source = 'uncategorized' OR category = 'Uncategorized') AS uncategorized_count,
            COALESCE(SUM(CASE WHEN category_source = 'uncategorized' OR category = 'Uncategorized'
                         THEN ABS(amount) END), 0) AS uncategorized_total,
            COUNTIF(category_source != 'uncategorized' AND category != 'Uncategorized') AS categorized_count,
            COUNT(*) AS total_count
        FROM {table}
        """
        summary_row = list(bq_client.query(summary_sql).result())[0]

        # Upload metadata — last upload date, newest transaction
        meta_sql = f"""
        SELECT
            MAX(upload_date) AS last_upload_date,
            MAX(transaction_date) AS newest_transaction_date,
            MIN(transaction_date) AS oldest_transaction_date
        FROM {table}
        """
        meta_row = list(bq_client.query(meta_sql).result())[0]
        last_upload_date = str(meta_row.last_upload_date) if meta_row.last_upload_date else None
        newest_txn_date = str(meta_row.newest_transaction_date) if meta_row.newest_transaction_date else None
        oldest_txn_date = str(meta_row.oldest_transaction_date) if meta_row.oldest_transaction_date else None

        # Last upload file info
        last_upload_file = None
        if last_upload_date:
            file_sql = f"""
            SELECT source_file, COUNT(*) AS row_count
            FROM {table}
            WHERE upload_date = @upload_date
            GROUP BY source_file
            ORDER BY row_count DESC
            LIMIT 1
            """
            file_cfg = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("upload_date", "STRING", last_upload_date),
            ])
            file_rows = list(bq_client.query(file_sql, job_config=file_cfg).result())
            if file_rows:
                last_upload_file = file_rows[0].source_file

        # Distinct categories (from both transactions and rules)
        cat_sql = f"""
        SELECT DISTINCT category FROM (
            SELECT DISTINCT category FROM {table} WHERE category != 'Uncategorized'
            UNION DISTINCT
            SELECT DISTINCT category FROM `{PROJECT_ID}.{DATASET_ID}.BankCategoryRules`
        ) ORDER BY category
        """
        try:
            categories = [r.category for r in bq_client.query(cat_sql).result()]
        except Exception:
            categories = []

        # Filtered count
        count_sql = f"SELECT COUNT(*) AS cnt FROM {table}{where_sql}"
        job_cfg = bigquery.QueryJobConfig(query_parameters=params[:])
        filtered_count = list(bq_client.query(count_sql, job_config=job_cfg).result())[0].cnt

        # Paginated rows
        rows_sql = f"""
        SELECT transaction_date, description, amount, transaction_type,
               category, category_source, vendor_normalized
        FROM {table}{where_sql}
        ORDER BY {order_sql}
        LIMIT @limit OFFSET @offset
        """
        row_params = params + [
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
            bigquery.ScalarQueryParameter("offset", "INT64", offset),
        ]
        job_cfg = bigquery.QueryJobConfig(query_parameters=row_params)
        rows = [
            {
                "transaction_date": str(r.transaction_date),
                "description": r.description,
                "amount": float(r.amount) if r.amount is not None else 0,
                "transaction_type": r.transaction_type,
                "category": r.category or "Uncategorized",
                "category_source": r.category_source or "uncategorized",
                "vendor_normalized": r.vendor_normalized or "",
            }
            for r in bq_client.query(rows_sql, job_config=job_cfg).result()
        ]

        return jsonify({
            "summary": {
                "uncategorized_count": summary_row.uncategorized_count,
                "uncategorized_total": round(float(summary_row.uncategorized_total), 2),
                "categorized_count": summary_row.categorized_count,
                "total_count": summary_row.total_count,
                "last_upload_date": last_upload_date,
                "last_upload_file": last_upload_file,
                "newest_transaction_date": newest_txn_date,
                "oldest_transaction_date": oldest_txn_date,
            },
            "categories": categories,
            "filtered_count": filtered_count,
            "limit": limit,
            "offset": offset,
            "transactions": rows,
        })

    except Exception as e:
        logger.error(f"Bank transactions API error: {e}")
        return jsonify({"error": str(e)}), 500


@bp.route("/api/bank-transactions/categorize", methods=["POST"])
def api_bank_transactions_categorize():
    """
    Bulk-update transaction categories.

    Request body:
    {
        "updates": [{
            "transaction_date": "2025-12-15",
            "description": "SOME VENDOR INC",
            "amount": -1234.56,
            "new_category": "2. Cost of Goods Sold/Food COGS",
            "vendor_normalized": "Some Vendor",
            "create_rule": true,
            "rule_keyword": "SOME VENDOR"
        }]
    }
    """
    try:
        data = request.get_json()
        if not data or "updates" not in data:
            return jsonify({"error": "JSON body with 'updates' array required"}), 400

        updates = data["updates"]
        if not updates:
            return jsonify({"error": "No updates provided"}), 400

        bq_client = bigquery.Client(project=PROJECT_ID)
        table = f"`{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`"
        cat_manager = BankCategoryManager(bq_client, DATASET_ID)

        updated = 0
        rules_created = 0
        errors: List[str] = []

        for item in updates:
            txn_date = item.get("transaction_date")
            desc = item.get("description")
            amount = item.get("amount")
            new_cat = item.get("new_category")

            if not all([txn_date, desc, new_cat]) or amount is None:
                errors.append(f"Skipped incomplete update: {item}")
                continue

            vendor = item.get("vendor_normalized", desc)

            # Update the transaction(s) in BigQuery
            update_sql = f"""
            UPDATE {table}
            SET category = @new_cat,
                category_source = 'manual',
                vendor_normalized = @vendor
            WHERE transaction_date = @txn_date
              AND description = @desc
              AND ROUND(amount, 2) = ROUND(@amount, 2)
            """
            job_cfg = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("new_cat", "STRING", new_cat),
                    bigquery.ScalarQueryParameter("vendor", "STRING", vendor),
                    bigquery.ScalarQueryParameter("txn_date", "STRING", txn_date),
                    bigquery.ScalarQueryParameter("desc", "STRING", desc),
                    bigquery.ScalarQueryParameter("amount", "FLOAT64", float(amount)),
                ]
            )
            result = bq_client.query(update_sql, job_config=job_cfg).result()
            affected = result.num_dml_affected_rows or 0
            updated += affected

            # Audit trail — log the categorization change
            if affected > 0:
                audit_sql = f"""
                INSERT INTO `{PROJECT_ID}.{DATASET_ID}.BankTransactions_audit`
                (transaction_date, description, amount, old_category, new_category,
                 vendor_normalized, changed_at, source)
                VALUES (@txn_date, @desc, @amount, @old_cat, @new_cat,
                        @vendor, CURRENT_TIMESTAMP(), 'manual')
                """
                old_cat = item.get("old_category", "")
                audit_cfg = bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter("txn_date", "STRING", txn_date),
                    bigquery.ScalarQueryParameter("desc", "STRING", desc),
                    bigquery.ScalarQueryParameter("amount", "FLOAT64", float(amount)),
                    bigquery.ScalarQueryParameter("old_cat", "STRING", old_cat),
                    bigquery.ScalarQueryParameter("new_cat", "STRING", new_cat),
                    bigquery.ScalarQueryParameter("vendor", "STRING", vendor),
                ])
                try:
                    bq_client.query(audit_sql, job_config=audit_cfg).result()
                except Exception as audit_err:
                    # Audit failure should not block categorization
                    logger.warning(f"Audit log failed: {audit_err}")

            # Optionally create a rule
            if item.get("create_rule") and item.get("rule_keyword"):
                try:
                    cat_manager.upsert_rule(item["rule_keyword"], new_cat, vendor)
                    rules_created += 1
                except Exception as rule_err:
                    errors.append(f"Rule creation failed for '{item['rule_keyword']}': {rule_err}")

        return jsonify({
            "status": "ok",
            "rows_updated": updated,
            "rules_created": rules_created,
            "errors": errors,
        })

    except Exception as e:
        logger.error(f"Bank transaction categorize error: {e}")
        return jsonify({"error": str(e)}), 500


@bp.route("/api/bank-transactions/delete", methods=["POST"])
def api_bank_transactions_delete():
    """
    Delete one or more transactions from BankTransactions_raw.

    Request body:
    {
        "deletes": [
            {"transaction_date": "2025-12-15", "description": "SOME VENDOR INC", "amount": -1234.56}
        ]
    }
    """
    try:
        data = request.get_json()
        if not data or "deletes" not in data:
            return jsonify({"error": "JSON body with 'deletes' array required"}), 400

        deletes = data["deletes"]
        if not deletes:
            return jsonify({"error": "No deletes provided"}), 400

        logger.info(f"Delete request received with {len(deletes)} item(s): {json.dumps(deletes[:3])}")

        bq_client = bigquery.Client(project=PROJECT_ID)
        table = f"`{PROJECT_ID}.{DATASET_ID}.BankTransactions_raw`"

        deleted = 0
        errors: List[str] = []

        for item in deletes:
            txn_date = item.get("transaction_date")
            desc = item.get("description")
            amount = item.get("amount")

            logger.info(f"Delete item: date={txn_date!r}, desc={desc!r}, amount={amount!r} (type={type(amount).__name__})")

            if not all([txn_date, desc]) or amount is None:
                errors.append(f"Skipped incomplete delete: {item}")
                logger.warning(f"Skipped incomplete delete: {item}")
                continue

            if amount == 0:
                # amount=0 from the API means NULL in BigQuery (see serialization in api_bank_transactions)
                delete_sql = f"""
                DELETE FROM {table}
                WHERE transaction_date = @txn_date
                  AND description = @desc
                  AND (amount IS NULL OR ROUND(amount, 2) = 0)
                """
                job_cfg = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("txn_date", "STRING", txn_date),
                        bigquery.ScalarQueryParameter("desc", "STRING", desc),
                    ]
                )
            else:
                delete_sql = f"""
                DELETE FROM {table}
                WHERE transaction_date = @txn_date
                  AND description = @desc
                  AND ROUND(amount, 2) = ROUND(@amount, 2)
                """
                job_cfg = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("txn_date", "STRING", txn_date),
                        bigquery.ScalarQueryParameter("desc", "STRING", desc),
                        bigquery.ScalarQueryParameter("amount", "FLOAT64", float(amount)),
                    ]
                )
            result = bq_client.query(delete_sql, job_config=job_cfg).result()
            affected = result.num_dml_affected_rows or 0
            logger.info(f"Delete result: {affected} row(s) affected for desc={desc!r}")
            deleted += affected

        return jsonify({
            "status": "ok",
            "rows_deleted": deleted,
            "errors": errors,
        })

    except Exception as e:
        logger.error(f"Bank transaction delete error: {e}")
        return jsonify({"error": str(e)}), 500
