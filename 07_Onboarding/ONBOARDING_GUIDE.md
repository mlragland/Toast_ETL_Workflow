
# Onboarding Guide for New Team Members

## Setup Prerequisites
- Google Cloud SDK installed
- Access to GCP project `toast-analytics`
- Docker installed for local testing
- Node.js and Python environment set up

## Clone the Repo
```bash
git clone https://github.com/example/toast-etl-app.git
cd toast-etl-app
```

## Environment Setup
- Copy `.env.template` to `.env`
- Fill in required secrets (SFTP creds, GCP service keys)

## First Run
```bash
docker build -t toast-etl .
docker run --env-file .env toast-etl
```

## Key Resources
- [Product Requirements](../01_Product_Management/PRD.md)
- [API Docs](../03_Development/API_CONTRACT.md)
- [Deployment Guide](../06_Operations/DEPLOYMENT.md)
