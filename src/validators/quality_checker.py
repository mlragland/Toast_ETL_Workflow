"""
Quality Checker for Toast ETL Pipeline.

Combines schema validation, business rule validation, and referential integrity
checks to provide comprehensive data quality assessment.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
import logging
from pathlib import Path

from .schema_enforcer import SchemaEnforcer
from .data_validator import DataValidator

logger = logging.getLogger(__name__)


class QualityChecker:
    """
    Comprehensive quality checker that combines all validation layers.
    
    Integrates schema enforcement, business rule validation, and referential
    integrity checks to provide a complete data quality assessment.
    """
    
    def __init__(self):
        """Initialize the Quality Checker."""
        self.schema_enforcer = SchemaEnforcer()
        self.data_validator = DataValidator()
        
        # Define referential integrity rules based on legacy analysis
        self.referential_rules = self._define_referential_rules()
    
    def _define_referential_rules(self) -> Dict[str, Any]:
        """
        Define referential integrity rules between files.
        
        Returns:
            Dictionary of referential integrity rules
        """
        return {
            "order_relationships": {
                # OrderDetails -> ItemSelectionDetails (order_id)
                "order_to_items": {
                    "parent_file": "OrderDetails.csv",
                    "child_file": "ItemSelectionDetails.csv",
                    "parent_key": "order_id",
                    "child_key": "order_id"
                },
                # OrderDetails -> PaymentDetails (order_id)
                "order_to_payments": {
                    "parent_file": "OrderDetails.csv",
                    "child_file": "PaymentDetails.csv",
                    "parent_key": "order_id",
                    "child_key": "order_id"
                }
            },
            "check_relationships": {
                # CheckDetails -> ItemSelectionDetails (check_id)
                "check_to_items": {
                    "parent_file": "CheckDetails.csv",
                    "child_file": "ItemSelectionDetails.csv",
                    "parent_key": "check_id",
                    "child_key": "check_id"
                },
                # CheckDetails -> PaymentDetails (check_id)
                "check_to_payments": {
                    "parent_file": "CheckDetails.csv",
                    "child_file": "PaymentDetails.csv",
                    "parent_key": "check_id",
                    "child_key": "check_id"
                },
                # CheckDetails -> KitchenTimings (check_number)
                "check_to_kitchen": {
                    "parent_file": "CheckDetails.csv",
                    "child_file": "KitchenTimings.csv",
                    "parent_key": "check_number",
                    "child_key": "check_number"
                }
            },
            "item_relationships": {
                # AllItemsReport -> ItemSelectionDetails (item_id)
                "items_to_selections": {
                    "parent_file": "AllItemsReport.csv",
                    "child_file": "ItemSelectionDetails.csv",
                    "parent_key": "item_id",
                    "child_key": "item_id"
                }
            }
        }
    
    def comprehensive_quality_check(self, file_data_map: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Perform comprehensive quality check on all files.
        
        Args:
            file_data_map: Dictionary mapping filenames to DataFrames
            
        Returns:
            Comprehensive quality report
        """
        quality_report = {
            "overall_status": "PASS",
            "file_reports": {},
            "referential_integrity": {},
            "cross_file_summary": {},
            "recommendations": [],
            "critical_issues": [],
            "warnings": [],
            "validation_timestamp": datetime.now().isoformat()
        }
        
        # Individual file validation
        critical_issues = 0
        total_warnings = 0
        
        for filename, df in file_data_map.items():
            file_report = self._validate_single_file(df, filename)
            quality_report["file_reports"][filename] = file_report
            
            # Aggregate issues
            if file_report["severity"] == "CRITICAL":
                critical_issues += 1
                quality_report["critical_issues"].extend(file_report.get("critical_errors", []))
            
            total_warnings += len(file_report.get("warnings", []))
            quality_report["warnings"].extend(file_report.get("warnings", []))
        
        # Cross-file referential integrity validation
        referential_results = self._validate_referential_integrity(file_data_map)
        quality_report["referential_integrity"] = referential_results
        
        # Add referential issues to overall count
        for relationship, result in referential_results.items():
            if not result.get("valid", True):
                critical_issues += 1
                quality_report["critical_issues"].extend(result.get("violations", []))
        
        # Generate cross-file summary
        quality_report["cross_file_summary"] = self._generate_cross_file_summary(file_data_map)
        
        # Determine overall status
        if critical_issues > 0:
            quality_report["overall_status"] = "CRITICAL"
        elif total_warnings > 0:
            quality_report["overall_status"] = "WARNING"
        
        # Generate recommendations
        quality_report["recommendations"] = self._generate_recommendations(quality_report)
        
        return quality_report
    
    def _validate_single_file(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """
        Validate a single file with all quality checks.
        
        Args:
            df: DataFrame to validate
            filename: Source filename
            
        Returns:
            Comprehensive file validation report
        """
        file_report = {
            "filename": filename,
            "row_count": len(df),
            "column_count": len(df.columns),
            "validation_timestamp": datetime.now().isoformat()
        }
        
        # Schema validation
        schema_report = self.schema_enforcer.generate_schema_report(df, filename)
        file_report["schema_validation"] = schema_report
        
        # Business rule validation
        business_rules_report = self.data_validator.validate_business_rules(df, filename)
        file_report["business_rules"] = business_rules_report
        
        # Anomaly detection
        anomalies_report = self.data_validator.detect_anomalies(df, filename)
        file_report["anomalies"] = anomalies_report
        
        # Aggregate results for file-level severity
        critical_errors = []
        warnings = []
        
        # Schema issues
        if not schema_report.get("valid", False):
            critical_errors.extend(schema_report.get("missing_columns", []))
            critical_errors.extend([f"Type mismatch: {m}" for m in schema_report.get("type_mismatches", [])])
            warnings.extend(schema_report.get("extra_columns", []))
        
        # Business rule issues
        if not business_rules_report.get("valid", False):
            critical_errors.extend(business_rules_report.get("errors", []))
        warnings.extend(business_rules_report.get("warnings", []))
        
        # Anomaly warnings
        duplicates = anomalies_report.get("duplicates", {})
        if duplicates.get("total_duplicate_rows", 0) > 0:
            warnings.append(f"Found {duplicates['total_duplicate_rows']} duplicate rows")
        
        outliers = anomalies_report.get("outliers", {})
        if outliers:
            for col, outlier_info in outliers.items():
                warnings.append(f"{col}: {outlier_info['count']} outliers detected")
        
        consistency_issues = anomalies_report.get("data_consistency", [])
        warnings.extend(consistency_issues)
        
        # Set severity
        file_report["critical_errors"] = critical_errors
        file_report["warnings"] = warnings
        file_report["severity"] = "CRITICAL" if critical_errors else ("WARNING" if warnings else "PASS")
        
        return file_report
    
    def _validate_referential_integrity(self, file_data_map: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Validate referential integrity between files.
        
        Args:
            file_data_map: Dictionary mapping filenames to DataFrames
            
        Returns:
            Referential integrity validation results
        """
        results = {}
        
        for relationship_type, relationships in self.referential_rules.items():
            for rel_name, rel_config in relationships.items():
                parent_file = rel_config["parent_file"]
                child_file = rel_config["child_file"]
                parent_key = rel_config["parent_key"]
                child_key = rel_config["child_key"]
                
                # Check if both files exist in the data
                if parent_file not in file_data_map or child_file not in file_data_map:
                    results[rel_name] = {
                        "valid": True,
                        "info": f"Files not available for relationship validation: {parent_file} -> {child_file}"
                    }
                    continue
                
                parent_df = file_data_map[parent_file]
                child_df = file_data_map[child_file]
                
                # Validate the relationship
                validation_result = self._validate_relationship(
                    parent_df, child_df, parent_key, child_key, parent_file, child_file
                )
                
                results[rel_name] = validation_result
        
        return results
    
    def _validate_relationship(self, parent_df: pd.DataFrame, child_df: pd.DataFrame,
                             parent_key: str, child_key: str, parent_file: str, child_file: str) -> Dict[str, Any]:
        """
        Validate a specific referential relationship.
        
        Args:
            parent_df: Parent DataFrame
            child_df: Child DataFrame
            parent_key: Key column in parent
            child_key: Key column in child
            parent_file: Parent filename
            child_file: Child filename
            
        Returns:
            Relationship validation result
        """
        violations = []
        
        # Check if key columns exist
        if parent_key not in parent_df.columns:
            return {
                "valid": False,
                "error": f"Parent key '{parent_key}' not found in {parent_file}"
            }
        
        if child_key not in child_df.columns:
            return {
                "valid": False,
                "error": f"Child key '{child_key}' not found in {child_file}"
            }
        
        # Get unique keys
        parent_keys = set(parent_df[parent_key].dropna().unique())
        child_keys = set(child_df[child_key].dropna().unique())
        
        # Find orphaned child records (child keys not in parent)
        orphaned_keys = child_keys - parent_keys
        if orphaned_keys:
            violations.append(f"Found {len(orphaned_keys)} orphaned records in {child_file} (keys not in {parent_file})")
        
        # Find missing child records (parent keys without children) - this might be expected
        missing_children = parent_keys - child_keys
        missing_percentage = (len(missing_children) / len(parent_keys) * 100) if parent_keys else 0
        
        return {
            "valid": len(violations) == 0,
            "violations": violations,
            "statistics": {
                "parent_unique_keys": len(parent_keys),
                "child_unique_keys": len(child_keys),
                "orphaned_children": len(orphaned_keys),
                "parents_without_children": len(missing_children),
                "missing_children_percentage": missing_percentage
            }
        }
    
    def _generate_cross_file_summary(self, file_data_map: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Generate summary statistics across all files.
        
        Args:
            file_data_map: Dictionary mapping filenames to DataFrames
            
        Returns:
            Cross-file summary statistics
        """
        summary = {
            "total_files": len(file_data_map),
            "total_records": sum(len(df) for df in file_data_map.values()),
            "file_record_counts": {filename: len(df) for filename, df in file_data_map.items()},
            "processing_date_consistency": {},
            "data_volume_analysis": {}
        }
        
        # Check processing date consistency
        processing_dates = {}
        for filename, df in file_data_map.items():
            if "processing_date" in df.columns:
                unique_dates = df["processing_date"].unique()
                processing_dates[filename] = list(unique_dates)
        
        summary["processing_date_consistency"] = processing_dates
        
        # Data volume analysis (identify unusually small/large files)
        if file_data_map:
            record_counts = [len(df) for df in file_data_map.values()]
            avg_records = sum(record_counts) / len(record_counts)
            
            volume_analysis = {}
            for filename, df in file_data_map.items():
                record_count = len(df)
                deviation = ((record_count - avg_records) / avg_records * 100) if avg_records > 0 else 0
                
                if abs(deviation) > 50:  # More than 50% deviation from average
                    volume_analysis[filename] = {
                        "record_count": record_count,
                        "deviation_percentage": deviation,
                        "status": "unusually_large" if deviation > 0 else "unusually_small"
                    }
            
            summary["data_volume_analysis"] = volume_analysis
        
        return summary
    
    def _generate_recommendations(self, quality_report: Dict[str, Any]) -> List[str]:
        """
        Generate actionable recommendations based on quality report.
        
        Args:
            quality_report: Comprehensive quality report
            
        Returns:
            List of recommendations
        """
        recommendations = []
        
        # Schema-based recommendations
        for filename, file_report in quality_report["file_reports"].items():
            schema_validation = file_report.get("schema_validation", {})
            
            if schema_validation.get("missing_columns"):
                recommendations.append(
                    f"{filename}: Add missing columns {schema_validation['missing_columns']} or update transformation logic"
                )
            
            if schema_validation.get("type_mismatches"):
                recommendations.append(
                    f"{filename}: Fix data type mismatches or update schema enforcement"
                )
        
        # Business rule recommendations
        if quality_report["overall_status"] == "CRITICAL":
            recommendations.append("URGENT: Resolve critical data quality issues before loading to BigQuery")
        
        # Volume-based recommendations
        volume_analysis = quality_report["cross_file_summary"].get("data_volume_analysis", {})
        for filename, analysis in volume_analysis.items():
            if analysis["status"] == "unusually_small":
                recommendations.append(
                    f"{filename}: Investigate unusually low record count ({analysis['record_count']} records)"
                )
        
        # Referential integrity recommendations
        for rel_name, result in quality_report["referential_integrity"].items():
            if not result.get("valid", True):
                recommendations.append(
                    f"Referential integrity: Fix {rel_name} relationship violations"
                )
        
        # Default recommendation if no specific issues
        if not recommendations and quality_report["overall_status"] == "PASS":
            recommendations.append("Data quality is excellent. Proceed with confidence to BigQuery loading.")
        
        return recommendations
    
    def validate_and_enforce(self, df: pd.DataFrame, filename: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Validate data and enforce corrections where possible.
        
        Args:
            df: DataFrame to validate and correct
            filename: Source filename
            
        Returns:
            Tuple of (corrected DataFrame, validation report)
        """
        # First, validate current state
        validation_report = self._validate_single_file(df, filename)
        
        # Attempt schema enforcement
        try:
            corrected_df, schema_warnings = self.schema_enforcer.enforce_schema_types(df, filename)
            validation_report["schema_corrections"] = {
                "applied": True,
                "warnings": schema_warnings
            }
        except Exception as e:
            logger.error(f"Schema enforcement failed for {filename}: {e}")
            corrected_df = df
            validation_report["schema_corrections"] = {
                "applied": False,
                "error": str(e)
            }
        
        # Re-validate after corrections
        post_correction_report = self._validate_single_file(corrected_df, filename)
        validation_report["post_correction_validation"] = post_correction_report
        
        return corrected_df, validation_report