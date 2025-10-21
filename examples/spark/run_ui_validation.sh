#!/bin/bash
# Complete setup and run script for long-running UI validation job

set -e

echo "=========================================="
echo "Spark UI Validation - Complete Setup"
echo "=========================================="
echo ""

# Check if we're in the right directory
if [ ! -f "run_long_job_ui_validation.py" ]; then
    echo "❌ Please run this from examples/spark directory"
    exit 1
fi

# Step 1: Check MinIO
echo "Step 1: Checking MinIO..."
if ! kubectl get pod -l app=minio 2>/dev/null | grep -q Running; then
    echo "  ⚠️  MinIO not running. Setting up..."
    ./setup_minio.sh
    echo ""
else
    echo "  ✓ MinIO is running"
fi
echo ""

# Step 2: Upload script
echo "Step 2: Uploading long-running job script..."
FILE_INFO=$(kubectl exec minio-client -- mc ls myminio/spark-scripts/long_running_job.py 2>/dev/null || echo "")

if [ -z "$FILE_INFO" ]; then
    echo "  Script not found in MinIO. Uploading..."
    chmod +x upload_long_job.sh
    ./upload_long_job.sh
elif echo "$FILE_INFO" | grep -q "0B"; then
    echo "  Script exists but is empty (0B). Re-uploading..."
    chmod +x upload_long_job.sh
    ./upload_long_job.sh
else
    echo "  ✓ Script already uploaded"
    echo "    $FILE_INFO"
fi
echo ""

# Step 3: Submit job
echo "Step 3: Submitting long-running job..."
echo "  This will take ~10 minutes to complete"
echo ""

# Run in foreground with --no-monitor flag (no interactive prompts)
# Capture output to get APP_NAME
OUTPUT=$(python run_long_job_ui_validation.py --no-monitor 2>&1)
echo "$OUTPUT"

# Extract app name from output
APP_NAME=$(echo "$OUTPUT" | grep "APP_NAME=" | cut -d= -f2)

# Fallback: try to get from kubectl
if [ -z "$APP_NAME" ]; then
    APP_NAME=$(kubectl get sparkapplications -o name 2>/dev/null | grep "long-job" | tail -1 | cut -d/ -f2)
fi

if [ -z "$APP_NAME" ]; then
    echo "  ⚠️  Could not find application. Check output above."
    exit 1
fi

echo "  ✓ Job submitted: $APP_NAME"
echo ""

# Step 4: Wait for driver pod
echo "Step 4: Waiting for driver pod to be ready..."
echo "  This may take 1-2 minutes..."
DRIVER_POD="${APP_NAME}-driver"

# Wait for pod to exist
for i in {1..60}; do
    if kubectl get pod $DRIVER_POD 2>/dev/null; then
        break
    fi
    sleep 2
done

# Wait for pod to be Running
kubectl wait --for=condition=ready pod/$DRIVER_POD --timeout=180s 2>/dev/null || {
    echo "  ⚠️  Pod taking longer than expected..."
    echo "  Monitor with: kubectl get pod $DRIVER_POD -w"
    echo ""
}

POD_STATUS=$(kubectl get pod $DRIVER_POD -o jsonpath='{.status.phase}' 2>/dev/null)
echo "  ✓ Driver pod status: $POD_STATUS"
echo ""

if [ "$POD_STATUS" != "Running" ]; then
    echo "  ⚠️  Pod not Running yet. Current status: $POD_STATUS"
    echo ""
    echo "  Monitor pod:"
    echo "    kubectl get pod $DRIVER_POD -w"
    echo ""
    echo "  Once Running, port-forward manually:"
    echo "    kubectl port-forward pod/$DRIVER_POD 4040:4040"
    echo ""
    exit 0
fi

# Step 5: Instructions for UI access
echo "=========================================="
echo "🎉 Job is Running!"
echo "=========================================="
echo ""
echo "Driver pod: $DRIVER_POD"
echo "Expected duration: ~10 minutes"
echo ""
echo "=========================================="
echo "TO ACCESS SPARK UI:"
echo "=========================================="
echo ""
echo "Open a NEW terminal and run:"
echo ""
echo "  kubectl port-forward pod/$DRIVER_POD 4040:4040"
echo ""
echo "Then open in your browser:"
echo ""
echo "  http://localhost:4040"
echo ""
echo "=========================================="
echo "WHAT TO EXPLORE:"
echo "=========================================="
echo ""
echo "Timeline:"
echo "  0:00 - Job starts"
echo "  2:00 - Stage 2 → CHECK STORAGE TAB! ⭐"
echo "  4:00 - Stage 3 → CHECK EXECUTORS TAB! (heavy shuffle)"
echo "  7:00 - Stage 4 → Check SQL tab"
echo "  9:00 - Stage 5 → Check DAG visualization"
echo " 10:00 - Job completes"
echo ""
echo "UI Tabs to explore:"
echo "  ✓ Jobs - See 6 jobs (one per stage)"
echo "  ✓ Stages - Monitor stage progress"
echo "  ✓ Storage - View cached data (after Stage 2)"
echo "  ✓ Executors - Monitor resources and shuffles"
echo "  ✓ SQL - Inspect DataFrame query plans"
echo "  ✓ Environment - View Spark configuration"
echo ""
echo "=========================================="
echo ""

# Offer to start port-forward
read -p "Start port-forward now? (y/n) " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo ""
    echo "Starting port-forward..."
    echo "Keep this terminal open!"
    echo "Open browser to: http://localhost:4040"
    echo ""
    echo "Press Ctrl+C to stop port-forward"
    echo ""
    sleep 2
    kubectl port-forward pod/$DRIVER_POD 4040:4040
else
    echo ""
    echo "To access UI later, run:"
    echo "  kubectl port-forward pod/$DRIVER_POD 4040:4040"
    echo ""
    echo "Monitor job with:"
    echo "  kubectl get sparkapplication $APP_NAME -w"
    echo ""
    echo "View logs with:"
    echo "  kubectl logs $DRIVER_POD -f"
    echo ""
fi
