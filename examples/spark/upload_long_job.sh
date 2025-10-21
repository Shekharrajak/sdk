#!/bin/bash
# Quick script to upload the long-running job script to MinIO

set -e

NAMESPACE="default"

echo "=================================================="
echo "Uploading Long-Running Job Script to MinIO"
echo "=================================================="
echo ""

# Check if MinIO is running
if ! kubectl get pod -n ${NAMESPACE} -l app=minio | grep -q Running; then
    echo "❌ MinIO is not running!"
    echo "   Run ./setup_minio.sh first"
    exit 1
fi

echo "✓ MinIO is running"
echo ""

# Check if minio-client exists
if ! kubectl get pod -n ${NAMESPACE} minio-client &>/dev/null; then
    echo "❌ minio-client pod not found!"
    echo "   Run ./setup_minio.sh first"
    exit 1
fi

echo "✓ MinIO client found"
echo ""

# Check if minio-client pod is running
POD_STATUS=$(kubectl get pod -n ${NAMESPACE} minio-client -o jsonpath='{.status.phase}' 2>/dev/null)

if [ "$POD_STATUS" != "Running" ]; then
    echo "  MinIO client pod is in '$POD_STATUS' state. Restarting..."

    # Delete the completed pod
    kubectl delete pod -n ${NAMESPACE} minio-client --ignore-not-found=true

    # Create a new one
    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: minio-client
  namespace: ${NAMESPACE}
spec:
  containers:
  - name: minio-client
    image: quay.io/minio/mc:latest
    command: ["sleep", "infinity"]
  restartPolicy: Never
EOF

    # Wait for it to be ready
    echo "  Waiting for MinIO client to be ready..."
    kubectl wait --for=condition=ready pod/minio-client -n ${NAMESPACE} --timeout=60s

    # Configure mc alias
    kubectl exec -n ${NAMESPACE} minio-client -- mc alias set myminio http://minio-service:9000 minioadmin minioadmin
    echo "  ✓ MinIO client restarted and configured"
fi

echo ""

# Upload the script
echo "Uploading long_running_job.py to MinIO..."

if [ ! -f "scripts/long_running_job.py" ]; then
    echo "❌ Script not found: scripts/long_running_job.py"
    echo "   Make sure you're in the examples/spark directory"
    exit 1
fi

# Upload to minio-client pod using pipe (more reliable than stdin redirection)
echo "  Copying script to minio-client pod..."
cat scripts/long_running_job.py | kubectl exec -i -n ${NAMESPACE} minio-client -- sh -c 'cat > /tmp/long_running_job.py'

# Verify the file has content in the pod
LINE_COUNT=$(kubectl exec -n ${NAMESPACE} minio-client -- wc -l /tmp/long_running_job.py | awk '{print $1}')
if [ "$LINE_COUNT" -eq 0 ]; then
    echo "❌ Upload failed: File is empty in pod"
    exit 1
fi
echo "  ✓ File copied to pod ($LINE_COUNT lines)"

# Upload to MinIO
echo "  Uploading to MinIO..."
kubectl exec -n ${NAMESPACE} minio-client -- mc cp /tmp/long_running_job.py myminio/spark-scripts/

echo "✓ Uploaded successfully"
echo ""

# Verify
echo "Verifying upload in MinIO..."
FILE_INFO=$(kubectl exec -n ${NAMESPACE} minio-client -- mc ls myminio/spark-scripts/long_running_job.py)
echo "$FILE_INFO"

# Check if file size is 0B (indicates empty file)
if echo "$FILE_INFO" | grep -q "0B"; then
    echo ""
    echo "❌ WARNING: File appears to be empty (0B) in MinIO!"
    echo "   This will cause Spark jobs to fail immediately."
    exit 1
fi

echo ""
echo "=================================================="
echo "✅ Setup Complete!"
echo "=================================================="
echo ""
echo "Now run:"
echo "  python run_long_job_ui_validation.py"
echo ""
