#!/bin/bash

# Toast ETL Pipeline - Deployment Script
# Automates infrastructure provisioning and application deployment for Phase 5

set -e  # Exit on any error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ID=${PROJECT_ID:-"your-gcp-project-id"}
REGION=${REGION:-"us-central1"}
ENVIRONMENT=${ENVIRONMENT:-"production"}
SERVICE_NAME="toast-etl-pipeline"

# Functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if required tools are installed
    for cmd in gcloud docker terraform; do
        if ! command -v $cmd &> /dev/null; then
            log_error "$cmd is not installed. Please install it and try again."
            exit 1
        fi
    done
    
    # Check if authenticated with Google Cloud
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
        log_error "Not authenticated with Google Cloud. Run 'gcloud auth login' first."
        exit 1
    fi
    
    # Check if project ID is set
    if [ "$PROJECT_ID" = "your-gcp-project-id" ]; then
        log_error "Please set the PROJECT_ID environment variable"
        exit 1
    fi
    
    log_success "Prerequisites check passed"
}

deploy_infrastructure() {
    log_info "Deploying infrastructure with Terraform..."
    
    cd infrastructure
    
    # Initialize Terraform
    terraform init
    
    # Plan the deployment
    terraform plan \
        -var="project_id=$PROJECT_ID" \
        -var="region=$REGION" \
        -var="environment=$ENVIRONMENT" \
        -out=tfplan
    
    # Apply the deployment
    terraform apply tfplan
    
    cd ..
    
    log_success "Infrastructure deployment completed"
}

build_and_push_image() {
    log_info "Building and pushing Docker image..."
    
    # Set up authentication for Artifact Registry
    gcloud auth configure-docker $REGION-docker.pkg.dev
    
    # Build the Docker image
    IMAGE_TAG="$REGION-docker.pkg.dev/$PROJECT_ID/toast-etl-pipeline/toast-etl:latest"
    
    docker build -t $IMAGE_TAG .
    
    # Push the image
    docker push $IMAGE_TAG
    
    log_success "Docker image built and pushed: $IMAGE_TAG"
    
    echo $IMAGE_TAG
}

deploy_cloud_run() {
    local image_tag=$1
    
    log_info "Deploying Cloud Run service..."
    
    # Deploy the service
    gcloud run deploy $SERVICE_NAME \
        --image=$image_tag \
        --platform=managed \
        --region=$REGION \
        --project=$PROJECT_ID \
        --allow-unauthenticated \
        --memory=4Gi \
        --cpu=2 \
        --timeout=3600 \
        --max-instances=10 \
        --min-instances=0 \
        --concurrency=1 \
        --set-env-vars="PROJECT_ID=$PROJECT_ID,ENVIRONMENT=$ENVIRONMENT" \
        --service-account="toast-etl-pipeline@$PROJECT_ID.iam.gserviceaccount.com"
    
    # Get the service URL
    SERVICE_URL=$(gcloud run services describe $SERVICE_NAME \
        --platform=managed \
        --region=$REGION \
        --project=$PROJECT_ID \
        --format="value(status.url)")
    
    log_success "Cloud Run service deployed: $SERVICE_URL"
    
    echo $SERVICE_URL
}

create_scheduler_jobs() {
    local service_url=$1
    
    log_info "Creating Cloud Scheduler jobs..."
    
    # Daily ETL job
    gcloud scheduler jobs create http toast-etl-daily \
        --location=$REGION \
        --schedule="30 4 * * *" \
        --time-zone="America/New_York" \
        --uri="$service_url/execute" \
        --http-method=POST \
        --headers="Content-Type=application/json" \
        --message-body='{"execution_date":"$(date +%Y-%m-%d)","environment":"'$ENVIRONMENT'","enable_validation":true,"quality_report":true}' \
        --oidc-service-account-email="toast-etl-pipeline@$PROJECT_ID.iam.gserviceaccount.com" \
        --oidc-token-audience="$service_url" \
        --max-retry-attempts=3 \
        --max-retry-duration=3600s \
        --min-backoff-duration=30s \
        --max-backoff-duration=600s \
        --max-doublings=4 \
        --project=$PROJECT_ID
    
    # Weekly validation job
    gcloud scheduler jobs create http toast-etl-weekly-validation \
        --location=$REGION \
        --schedule="0 5 * * 1" \
        --time-zone="America/New_York" \
        --uri="$service_url/validate-weekly" \
        --http-method=POST \
        --headers="Content-Type=application/json" \
        --message-body='{"validation_type":"comprehensive","date_range_days":7,"environment":"'$ENVIRONMENT'","deep_analysis":true}' \
        --oidc-service-account-email="toast-etl-pipeline@$PROJECT_ID.iam.gserviceaccount.com" \
        --oidc-token-audience="$service_url" \
        --max-retry-attempts=2 \
        --max-retry-duration=1800s \
        --min-backoff-duration=60s \
        --max-backoff-duration=300s \
        --max-doublings=3 \
        --project=$PROJECT_ID
    
    log_success "Cloud Scheduler jobs created"
}

test_deployment() {
    local service_url=$1
    
    log_info "Testing deployment..."
    
    # Test health endpoint
    response=$(curl -s -o /dev/null -w "%{http_code}" "$service_url/health")
    if [ "$response" = "200" ]; then
        log_success "Health check passed"
    else
        log_error "Health check failed (HTTP $response)"
        return 1
    fi
    
    # Test status endpoint
    response=$(curl -s -o /dev/null -w "%{http_code}" "$service_url/status")
    if [ "$response" = "200" ]; then
        log_success "Status endpoint working"
    else
        log_error "Status endpoint failed (HTTP $response)"
        return 1
    fi
    
    log_success "Deployment tests passed"
}

cleanup() {
    log_info "Cleaning up temporary files..."
    rm -f infrastructure/tfplan
    log_success "Cleanup completed"
}

main() {
    log_info "Starting Toast ETL Pipeline deployment..."
    log_info "Project: $PROJECT_ID"
    log_info "Region: $REGION"
    log_info "Environment: $ENVIRONMENT"
    
    # Run deployment steps
    check_prerequisites
    
    # Deploy infrastructure
    deploy_infrastructure
    
    # Build and push Docker image
    image_tag=$(build_and_push_image)
    
    # Deploy Cloud Run service
    service_url=$(deploy_cloud_run $image_tag)
    
    # Create scheduler jobs
    create_scheduler_jobs $service_url
    
    # Test the deployment
    test_deployment $service_url
    
    # Cleanup
    cleanup
    
    log_success "🎉 Toast ETL Pipeline deployment completed successfully!"
    log_info "Service URL: $service_url"
    log_info "Daily ETL runs at 4:30 AM EST"
    log_info "Weekly validation runs at 5:00 AM EST on Mondays"
    
    # Display useful commands
    echo ""
    log_info "Useful commands:"
    echo "  View logs: gcloud logging read 'resource.type=cloud_run_revision AND resource.labels.service_name=$SERVICE_NAME' --limit=50 --project=$PROJECT_ID"
    echo "  Trigger manual run: curl -X POST -H 'Content-Type: application/json' '$service_url/execute' -d '{\"execution_date\":\"$(date +%Y-%m-%d)\"}'"
    echo "  Check scheduler jobs: gcloud scheduler jobs list --location=$REGION --project=$PROJECT_ID"
}

# Run main function
main "$@" 