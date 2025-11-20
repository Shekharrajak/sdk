#!/usr/bin/env bash
#
# Setup Spark Connect Server in Kubernetes for Testing
#
# This script:
# 1. Sets up Kubernetes cluster using existing setup_test_environment.sh
# 2. Deploys Spark Connect server
# 3. Sets up port forwarding for local access
# 4. Verifies the installation

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_NAME="${CLUSTER_NAME:-spark-test}"

print_step() {
    echo -e "${GREEN}==>${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}WARNING:${NC} $1"
}

print_error() {
    echo -e "${RED}ERROR:${NC} $1"
}

# Setup Kubernetes cluster with Spark Operator
setup_kubernetes_cluster() {
    print_step "Setting up Kubernetes cluster..."

    if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
        print_warning "Cluster '${CLUSTER_NAME}' already exists"

        # Export kubeconfig to ensure context is set
        kind export kubeconfig --name ${CLUSTER_NAME} 2>/dev/null || true

        # Verify context is set
        if ! kubectl config get-contexts | grep -q "kind-${CLUSTER_NAME}"; then
            print_error "Failed to set kubectl context"
            print_step "Please run: kind export kubeconfig --name ${CLUSTER_NAME}"
            exit 1
        fi
    else
        print_step "Running setup_test_environment.sh..."
        bash "${SCRIPT_DIR}/setup_test_environment.sh"
    fi

    print_step "Kubernetes cluster ready"
}

# Deploy Spark Connect server
deploy_spark_connect() {
    print_step "Deploying Spark Connect server..."

    kubectl apply -f "${SCRIPT_DIR}/spark-connect-server.yaml"

    print_step "Waiting for Spark Connect server to be ready..."
    kubectl wait --for=condition=ready pod \
        -l app=spark-connect \
        -n default \
        --timeout=300s

    print_step "Spark Connect server deployed successfully"
}

# Setup port forwarding
setup_port_forwarding() {
    print_step "Setting up port forwarding..."

    # Kill any existing port forwarding on port 30000
    pkill -f "kubectl.*port-forward.*30000" || true
    sleep 2

    # Get the Spark Connect pod name
    POD_NAME=$(kubectl get pods -l app=spark-connect -n default -o jsonpath='{.items[0].metadata.name}')

    print_step "Port forwarding from localhost:30000 to Spark Connect pod..."

    # Start port forwarding in background
    kubectl port-forward -n default "pod/${POD_NAME}" 30000:15002 > /dev/null 2>&1 &
    PF_PID=$!

    # Wait for port forwarding to be established
    sleep 3

    # Verify port forwarding
    if lsof -i :30000 > /dev/null 2>&1; then
        print_step "Port forwarding established (PID: ${PF_PID})"
        echo "${PF_PID}" > /tmp/spark-connect-port-forward.pid
        print_warning "Port forwarding running in background"
        print_warning "To stop: kill \$(cat /tmp/spark-connect-port-forward.pid)"
    else
        print_error "Failed to establish port forwarding"
        return 1
    fi
}

# Verify installation
verify_installation() {
    print_step "Verifying installation..."

    echo ""
    echo "Cluster: ${CLUSTER_NAME}"
    echo "Spark Connect Pod:"
    kubectl get pods -l app=spark-connect -n default

    echo ""
    echo "Spark Connect Service:"
    kubectl get svc spark-connect -n default

    echo ""
    print_step "Installation complete!"
}

# Print usage instructions
print_usage() {
    echo ""
    echo "=" * 80
    echo " Spark Connect Setup Complete"
    echo "=" * 80
    echo ""
    echo "Spark Connect server is now running and accessible at:"
    echo "  URL: sc://localhost:30000"
    echo ""
    echo "Next steps:"
    echo ""
    echo "1. Install PySpark with Connect support (if not already installed):"
    echo "   pip install 'pyspark[connect]>=3.4.0'"
    echo ""
    echo "2. Run the interactive demo:"
    echo "   python examples/spark/ipython_spark_connect_demo.py"
    echo ""
    echo "3. Or run manual tests:"
    echo "   python examples/spark/ipython_spark_connect_demo.py --manual"
    echo ""
    echo "4. Or launch IPython for step-by-step experimentation:"
    echo "   cd examples/spark"
    echo "   python -c 'from ipython_spark_connect_demo import *; import IPython; IPython.embed()'"
    echo ""
    echo "5. View Spark UI:"
    echo "   kubectl port-forward -n default svc/spark-connect 4040:4040"
    echo "   Open: http://localhost:4040"
    echo ""
    echo "Cluster management:"
    echo "  - View logs: kubectl logs -l app=spark-connect -n default -f"
    echo "  - Restart: kubectl rollout restart deployment/spark-connect -n default"
    echo "  - Delete: kubectl delete -f examples/spark/spark-connect-server.yaml"
    echo "  - Cleanup cluster: kind delete cluster --name ${CLUSTER_NAME}"
    echo ""
    echo "Port forwarding PID: $(cat /tmp/spark-connect-port-forward.pid 2>/dev/null || echo 'N/A')"
    echo "Stop port forwarding: kill \$(cat /tmp/spark-connect-port-forward.pid)"
}

# Main
main() {
    echo "========================================"
    echo " Spark Connect Server Setup"
    echo "========================================"
    echo ""

    setup_kubernetes_cluster
    deploy_spark_connect
    setup_port_forwarding
    verify_installation
    print_usage
}

# Cleanup function
cleanup() {
    print_step "Cleaning up Spark Connect resources..."

    # Stop port forwarding
    if [ -f /tmp/spark-connect-port-forward.pid ]; then
        PF_PID=$(cat /tmp/spark-connect-port-forward.pid)
        if ps -p ${PF_PID} > /dev/null 2>&1; then
            kill ${PF_PID} 2>/dev/null || true
            print_step "Port forwarding stopped"
        fi
        rm -f /tmp/spark-connect-port-forward.pid
    fi

    # Delete Spark Connect deployment
    kubectl delete -f "${SCRIPT_DIR}/spark-connect-server.yaml" --ignore-not-found=true

    print_step "Cleanup complete"
}

# Handle script arguments
case "${1:-setup}" in
    setup)
        main
        ;;
    cleanup)
        cleanup
        ;;
    restart)
        cleanup
        sleep 2
        main
        ;;
    *)
        echo "Usage: $0 {setup|cleanup|restart}"
        echo ""
        echo "Commands:"
        echo "  setup   - Setup cluster and deploy Spark Connect (default)"
        echo "  cleanup - Remove Spark Connect and stop port forwarding"
        echo "  restart - Cleanup and setup again"
        exit 1
        ;;
esac
