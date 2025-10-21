#!/bin/bash
#
# Open Spark UI for a running application
#

APP_NAME=$1
NAMESPACE=${2:-default}
PORT=${3:-4040}

if [ -z "$APP_NAME" ]; then
    echo "Usage: $0 <app-name> [namespace] [port]"
    echo ""
    echo "Examples:"
    echo "  $0 test-spark-pi"
    echo "  $0 test-spark-pi default 4040"
    echo ""
    exit 1
fi

echo "=========================================="
echo "Spark UI Access"
echo "=========================================="
echo "Application: $APP_NAME"
echo "Namespace: $NAMESPACE"
echo "Local Port: $PORT"
echo ""

# Check if driver pod exists
POD_STATUS=$(kubectl get pod $APP_NAME-driver -n $NAMESPACE -o jsonpath='{.status.phase}' 2>/dev/null)

if [ -z "$POD_STATUS" ]; then
    echo "✗ Driver pod not found: $APP_NAME-driver"
    echo ""
    echo "Check if application exists:"
    echo "  kubectl get sparkapplication -n $NAMESPACE"
    exit 1
fi

echo "Driver Pod Status: $POD_STATUS"

if [ "$POD_STATUS" != "Running" ]; then
    echo ""
    echo "⚠️  Warning: Driver pod is not in Running state"
    echo "   Spark UI may not be accessible"
    echo ""
fi

echo ""
echo "Starting port-forward..."
echo "=========================================="
echo ""
echo "✓ Spark UI will be available at:"
echo ""
echo "  http://localhost:$PORT"
echo ""
echo "=========================================="
echo ""
echo "In the Spark UI you can view:"
echo "  • Jobs Tab       - See all Spark jobs and their status"
echo "  • Stages Tab     - View DAG visualization and task details"
echo "  • Storage Tab    - Check cached RDDs/DataFrames"
echo "  • Environment    - View Spark configuration"
echo "  • Executors Tab  - Monitor executor resources and tasks"
echo "  • SQL Tab        - See DataFrame/SQL query execution plans"
echo ""
echo "Press Ctrl+C to stop port forwarding"
echo "=========================================="
echo ""

# Start port forwarding
kubectl port-forward $APP_NAME-driver $PORT:4040 -n $NAMESPACE
