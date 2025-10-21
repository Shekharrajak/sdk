#!/usr/bin/env bash
#
# Clean up all Spark applications and orphaned pods
#

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_step() {
    echo -e "${GREEN}➜${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

NAMESPACE="${1:-default}"

echo "=========================================="
echo " Cleaning up Spark applications"
echo " Namespace: $NAMESPACE"
echo "=========================================="
echo ""

# Check what exists
print_step "Current state:"
echo ""
echo "SparkApplications:"
kubectl get sparkapplications -n $NAMESPACE 2>/dev/null || echo "  (none)"
echo ""
echo "Driver pods:"
kubectl get pods -n $NAMESPACE -l spark-role=driver 2>/dev/null || echo "  (none)"
echo ""
echo "Executor pods:"
kubectl get pods -n $NAMESPACE -l spark-role=executor 2>/dev/null || echo "  (none)"
echo ""

read -p "Delete all Spark applications and pods? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cancelled."
    exit 0
fi

# Delete SparkApplications
print_step "Deleting SparkApplications..."
if kubectl get sparkapplications -n $NAMESPACE &>/dev/null; then
    kubectl delete sparkapplications -n $NAMESPACE --all --timeout=30s || true
else
    echo "  No SparkApplications found"
fi

# Delete driver pods (force delete)
print_step "Force deleting driver pods..."
if kubectl get pods -n $NAMESPACE -l spark-role=driver &>/dev/null; then
    kubectl delete pods -n $NAMESPACE -l spark-role=driver --force --grace-period=0 --timeout=30s || true
else
    echo "  No driver pods found"
fi

# Delete executor pods (force delete)
print_step "Force deleting executor pods..."
if kubectl get pods -n $NAMESPACE -l spark-role=executor &>/dev/null; then
    kubectl delete pods -n $NAMESPACE -l spark-role=executor --force --grace-period=0 --timeout=30s || true
else
    echo "  No executor pods found"
fi

# Delete orphaned ConfigMaps
print_step "Cleaning up ConfigMaps..."
if kubectl get configmaps -n $NAMESPACE -l sparkoperator.k8s.io/app-name &>/dev/null; then
    kubectl delete configmaps -n $NAMESPACE -l sparkoperator.k8s.io/app-name --timeout=30s || true
else
    echo "  No Spark ConfigMaps found"
fi

echo ""
print_step "Cleanup complete!"
echo ""
echo "Verification:"
kubectl get sparkapplications -n $NAMESPACE 2>/dev/null || echo "  ✓ No SparkApplications"
kubectl get pods -n $NAMESPACE -l spark-role 2>/dev/null || echo "  ✓ No Spark pods"
echo ""
echo "You can now submit new applications:"
echo "  python test_spark_client_integration.py"
