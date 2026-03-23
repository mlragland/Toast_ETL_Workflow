# Toast Pipeline Deployment - Complete Beginner Guide

Copy and paste these commands exactly as shown. Run them one section at a time.

---

## PART 1: Install Claude Code

Open your Mac Terminal app and run:

```bash
# Check if Node.js is installed
node --version
```

If you see "command not found", install Node.js first:
```bash
# Install Homebrew if you don't have it
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Node.js
brew install node
```

Now install Claude Code:
```bash
npm install -g @anthropic-ai/claude-code
```

Verify it installed:
```bash
claude --version
```

---

## PART 2: Install Google Cloud SDK

Check if you have it:
```bash
gcloud --version
```

If "command not found", install it:
```bash
# Download and install
curl https://sdk.cloud.google.com | bash

# Restart your terminal, then run:
exec -l $SHELL

# Initialize gcloud
gcloud init
```

---

## PART 3: Authenticate with Google Cloud

```bash
# Login to your Google account (opens browser)
gcloud auth login

# Set up application default credentials (opens browser again)
gcloud auth application-default login

# Set your project
gcloud config set project toast-analytics-444116

# Verify
gcloud config get-value project
```

You should see: `toast-analytics-444116`

---

## PART 4: Download and Set Up the Pipeline Files

```bash
# Create project folder
mkdir -p ~/projects/toast-pipeline
cd ~/projects/toast-pipeline
```

Now download the zip file I provided in this chat and move it to this folder:
```bash
# Move the downloaded zip (adjust path if your Downloads folder is different)
mv ~/Downloads/toast-pipeline.zip ~/projects/toast-pipeline/

# Unzip it
unzip toast-pipeline.zip

# Verify files exist
ls -la
```

You should see: `main.py`, `deploy.sh`, `Dockerfile`, `requirements.txt`, `CLAUDE.md`, etc.

---

## PART 5: Locate Your Toast SFTP Private Key

You need the private key file you use to connect to Toast SFTP. It's probably:
- In your `~/.ssh/` folder
- Or wherever you saved it when Toast sent it

Find it:
```bash
# Check common locations
ls -la ~/.ssh/*.pem
ls -la ~/Downloads/*.pem
ls -la ~/Desktop/*.pem
```

Once you find it, note the full path. Example: `/Users/maurice/.ssh/toast-key.pem`

---

## PART 6: Start Claude Code

```bash
# Make sure you're in the project folder
cd ~/projects/toast-pipeline

# Start Claude Code
claude
```

**What happens:** 
- Claude Code will open in your terminal
- It reads the CLAUDE.md file automatically
- You'll see a prompt where you can type

---

## PART 7: Deployment Commands (Inside Claude Code)

Once Claude Code is running, type these prompts one at a time. Wait for each to complete before moving to the next.

### Step 7.1: Verify Setup
Type this into Claude Code:
```
Check if I'm authenticated with gcloud and show me the current project
```

### Step 7.2: Enable Required APIs
Type this:
```
Enable these GCP APIs:
- Cloud Run
- Cloud Build  
- Cloud Scheduler
- Secret Manager
- BigQuery
```

### Step 7.3: Add Your SFTP Key
Type this (replace the path with YOUR actual key path):
```
Add my Toast SFTP private key to Secret Manager. My key file is at: /Users/maurice/.ssh/toast-key.pem
```

### Step 7.4: Deploy to Cloud Run
Type this:
```
Build and deploy the Toast ETL pipeline to Cloud Run using the Dockerfile in this directory. Use region us-central1, 2GB memory, 2 CPUs, 10 minute timeout.
```

This will take 3-5 minutes. You'll see build logs scrolling.

### Step 7.5: Set Up Permissions
Type this:
```
Set up IAM permissions:
1. Create service account toast-etl-scheduler
2. Grant it Cloud Run invoker role on toast-etl-pipeline
3. Grant the default compute service account BigQuery Data Editor and Secret Manager accessor roles
```

### Step 7.6: Create the Daily Scheduler
Type this:
```
Create a Cloud Scheduler job called toast-etl-daily that:
- Runs at 6 AM Central Time every day
- POSTs to the /run endpoint of toast-etl-pipeline
- Uses OIDC authentication with the toast-etl-scheduler service account
```

### Step 7.7: Test the Pipeline
Type this:
```
Test the pipeline:
1. Call the health endpoint and show the response
2. Run the pipeline for yesterday's date
3. Show me the results
```

### Step 7.8: Backfill Missing Data
Type this:
```
Backfill Toast data from October 11, 2025 through January 29, 2025. This will take a while - that's okay.
```

---

## PART 8: Exit Claude Code

When you're done, type:
```
exit
```

Or press `Ctrl+C`

---

## PART 9: Verify Everything Worked

Back in regular terminal (not Claude Code):

```bash
# Check Cloud Run service exists
gcloud run services list --region=us-central1

# Check scheduler job exists  
gcloud scheduler jobs list --location=us-central1

# Check secret exists
gcloud secrets list
```

---

## Troubleshooting

### "claude: command not found"
```bash
# Check npm global bin path
npm bin -g

# Add to your PATH (add this line to ~/.zshrc or ~/.bashrc)
export PATH="$PATH:$(npm bin -g)"

# Reload terminal
source ~/.zshrc
```

### "gcloud: command not found" after install
```bash
# Restart terminal completely, or run:
source ~/google-cloud-sdk/path.bash.inc
```

### "Permission denied" in Claude Code
Type to Claude Code:
```
What IAM permissions am I missing? List them and grant them to my account.
```

### "SFTP connection failed" during pipeline run
Your key file path is probably wrong. Type to Claude Code:
```
List all .pem files on my computer and help me find my Toast SFTP key
```

### Claude Code seems stuck
Press `Ctrl+C` to cancel, then type your prompt again.

---

## Daily Operations (After Deployment)

To check on your pipeline later:

```bash
cd ~/projects/toast-pipeline
claude
```

Then ask things like:
- "Show me the logs from today's pipeline run"
- "Did yesterday's data load successfully?"
- "Run the pipeline again for January 28"
- "What's the row count in OrderDetails_raw?"

---

## Summary

| Step | What It Does |
|------|--------------|
| Parts 1-3 | Install tools (one-time) |
| Part 4 | Download pipeline code |
| Part 5 | Find your SFTP key |
| Parts 6-7 | Deploy with Claude Code |
| Part 8-9 | Verify and exit |

Total time: ~30-45 minutes for first deployment
