#!/bin/bash
# Quick test to access Spark UI using driver pod port-forward

echo "=========================================="
echo "Quick Spark UI Access Test"
echo "=========================================="
echo ""

# Run a simple test app
echo "Step 1: Submitting test application..."
python test_ui_minimal.py &
PYTHON_PID=$!

# Wait for submission
sleep 15

# Find the app name
APP_NAME=$(kubectl get sparkapplications -o name | grep "test-ui" | head -1 | cut -d/ -f2)

if [ -z "$APP_NAME" ]; then
    echo "❌ No test application found"
    echo "   Run manually: python test_ui_minimal.py"
    exit 1
fi

echo "✓ Application found: $APP_NAME"
echo ""

# Wait for driver pod
echo "Step 2: Waiting for driver pod to be ready..."
DRIVER_POD="${APP_NAME}-driver"
kubectl wait --for=condition=ready pod/$DRIVER_POD --timeout=60s 2>/dev/null || {
    echo "   Still waiting for pod..."
    sleep 10
}

# Check if pod exists
POD_STATUS=$(kubectl get pod $DRIVER_POD -o jsonpath='{.status.phase}' 2>/dev/null)

if [ "$POD_STATUS" != "Running" ]; then
    echo "⚠️  Driver pod not Running yet (status: $POD_STATUS)"
    echo ""
    echo "Monitor with:"
    echo "   kubectl get pods -w | grep $APP_NAME"
    echo ""
    echo "Once Running, use:"
    echo "   kubectl port-forward pod/$DRIVER_POD 4040:4040"
    exit 0
fi

echo "✓ Driver pod is Running"
echo ""

# Check if service exists
echo "Step 3: Checking for UI service..."
if kubectl get svc ${APP_NAME}-ui-svc 2>/dev/null; then
    echo "✅ UI service exists! (Operator created it successfully)"
    echo ""
    echo "Access UI using service:"
    echo "   kubectl port-forward svc/${APP_NAME}-ui-svc 4040:4040"
else
    echo "❌ UI service does NOT exist (as expected with v2.0.2-rc.0)"
fi
echo ""

# Port-forward to driver pod
echo "Step 4: Setting up port-forward to driver pod..."
echo ""
echo "=========================================="
echo "🌐 SPARK UI ACCESS"
echo "=========================================="
echo ""
echo "Run this command in another terminal:"
echo ""
echo "  kubectl port-forward pod/$DRIVER_POD 4040:4040"
echo ""
echo "Then open in your browser:"
echo "  http://localhost:4040"
echo ""
echo "=========================================="
echo ""

# Offer to start port-forward
read -p "Start port-forward now? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Starting port-forward..."
    echo "Press Ctrl+C to stop"
    kubectl port-forward pod/$DRIVER_POD 4040:4040
else
    echo ""
    echo "Manual commands:"
    echo "  kubectl port-forward pod/$DRIVER_POD 4040:4040"
    echo "  # Then open: http://localhost:4040"
fi
