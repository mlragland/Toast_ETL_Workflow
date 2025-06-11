
# Product Requirements Document (PRD)

## Overview
The Toast ETL Automation system aims to streamline ingestion, transformation, and reporting of daily POS data from Toast systems.

## Goals
- Reduce manual effort by automating SFTP ingestion
- Improve data availability and reliability
- Enable easy validation and traceability of ETL runs

## User Stories
- As an Ops Manager, I want to be notified if data ingestion fails so I can take action.
- As a Data Analyst, I want clean, validated data in BigQuery every morning to prepare reports.
- As a Product Manager, I want historical files ingested without duplicates for accurate trend analysis.

## Metrics of Success
- <5% failure rate
- <10 minutes end-to-end processing
- Dashboard adoption by analytics team

## Milestones
See project milestone documentation in Architecture/README.md.
