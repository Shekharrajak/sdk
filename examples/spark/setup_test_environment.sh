#!/usr/bin/env bash
#
# Setup script for running Spark Operator in Kind cluster for testing
#
# This script:
# 1. Creates a Kind cluster
# 2. Installs Spark Operator
# 3. Sets up service accounts and RBAC
# 4. Verifies the installation

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
CLUSTER_NAME="${CLUSTER_NAME:-spark-test}"
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-spark-operator}"
SPARK_NAMESPACE="${SPARK_NAMESPACE:-default}"
SPARK_OPERATOR_VERSION="${SPARK_OPERATOR_VERSION:-v2.0.2-rc.0}"
SPARK_OPERATOR_CHART_VERSION="${SPARK_OPERATOR_CHART_VERSION:-2.0.2-rc.0}"

print_step() {
    echo -e "${GREEN}==>${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}WARNING:${NC} $1"
}

print_error() {
    echo -e "${RED}ERROR:${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    print_step "Checking prerequisites..."

    if ! command -v kind &> /dev/null; then
        print_error "kind not found. Please install kind: https://kind.sigs.k8s.io/docs/user/quick-start/"
        exit 1
    fi

    if ! command -v kubectl &> /dev/null; then
        print_error "kubectl not found. Please install kubectl"
        exit 1
    fi

    if ! command -v helm &> /dev/null; then
        print_warning "helm not found. Will use kubectl apply instead"
    fi

    print_step "Prerequisites OK"
}

# Create Kind cluster
create_cluster() {
    print_step "Creating Kind cluster '${CLUSTER_NAME}'..."

    if kind get clusters | grep -q "^${CLUSTER_NAME}$"; then
        print_warning "Cluster '${CLUSTER_NAME}' already exists. Skipping creation."
        return
    fi

    cat <<EOF | kind create cluster --name ${CLUSTER_NAME} --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  kubeadmConfigPatches:
  - |
    kind: InitConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        node-labels: "ingress-ready=true"
  extraPortMappings:
  - containerPort: 30000
    hostPort: 30000
    protocol: TCP
- role: worker
- role: worker
EOF

    print_step "Cluster created successfully"
}

# Install Spark Operator using Helm
install_spark_operator_helm() {
    print_step "Installing Spark Operator using Helm..."

    # Add Spark Operator Helm repo
    helm repo add spark-operator https://kubeflow.github.io/spark-operator
    helm repo update

    # Create namespace
    kubectl create namespace ${OPERATOR_NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -

    # Install Spark Operator
    helm upgrade --install spark-operator spark-operator/spark-operator \
        --namespace ${OPERATOR_NAMESPACE} \
        --version ${SPARK_OPERATOR_CHART_VERSION} \
        --set webhook.enable=true \
        --set webhook.port=8080 \
        --wait || {
        print_warning "Helm install with specific version failed, trying latest..."
        helm upgrade --install spark-operator spark-operator/spark-operator \
            --namespace ${OPERATOR_NAMESPACE} \
            --set webhook.enable=true \
            --set webhook.port=8080 \
            --wait
    }

    print_step "Spark Operator installed successfully"
}

# Install Spark Operator using kubectl (fallback)
install_spark_operator_kubectl() {
    print_step "Installing Spark Operator using kubectl..."

    # Create namespace
    kubectl create namespace ${OPERATOR_NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -

    # Try to install from GitHub releases
    print_step "Downloading Spark Operator manifests..."

    MANIFEST_URL="https://github.com/kubeflow/spark-operator/releases/download/${SPARK_OPERATOR_VERSION}/spark-operator.yaml"

    if curl -fsSL "${MANIFEST_URL}" -o /tmp/spark-operator.yaml 2>/dev/null; then
        print_step "Installing from release manifest..."
        kubectl apply -f /tmp/spark-operator.yaml -n ${OPERATOR_NAMESPACE}
        rm -f /tmp/spark-operator.yaml
    else
        print_warning "Release manifest not found, using main branch..."

        # Fallback: Install CRDs and operator from main branch
        kubectl apply -f https://raw.githubusercontent.com/kubeflow/spark-operator/master/config/crd/bases/sparkoperator.k8s.io_sparkapplications.yaml
        kubectl apply -f https://raw.githubusercontent.com/kubeflow/spark-operator/master/config/crd/bases/sparkoperator.k8s.io_scheduledsparkapplications.yaml

        # Install operator deployment
        cat <<EOF | kubectl apply -n ${OPERATOR_NAMESPACE} -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark-operator
  namespace: ${OPERATOR_NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: spark-operator
rules:
- apiGroups: [""]
  resources: ["pods", "services", "configmaps"]
  verbs: ["create", "get", "list", "watch", "update", "patch", "delete"]
- apiGroups: [""]
  resources: ["events"]
  verbs: ["create", "update", "patch"]
- apiGroups: ["sparkoperator.k8s.io"]
  resources: ["sparkapplications", "scheduledsparkapplications"]
  verbs: ["create", "get", "list", "watch", "update", "patch", "delete"]
- apiGroups: ["sparkoperator.k8s.io"]
  resources: ["sparkapplications/status", "scheduledsparkapplications/status"]
  verbs: ["get", "update", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: spark-operator
subjects:
- kind: ServiceAccount
  name: spark-operator
  namespace: ${OPERATOR_NAMESPACE}
roleRef:
  kind: ClusterRole
  name: spark-operator
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spark-operator
  namespace: ${OPERATOR_NAMESPACE}
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: spark-operator
  template:
    metadata:
      labels:
        app.kubernetes.io/name: spark-operator
    spec:
      serviceAccountName: spark-operator
      containers:
      - name: spark-operator
        image: docker.io/kubeflow/spark-operator:v2.0.2-rc.0
        imagePullPolicy: IfNotPresent
        args:
        - -v=2
        - -namespace=${SPARK_NAMESPACE}
        - -enable-webhook=false
EOF
    fi

    print_step "Spark Operator installed successfully"
}

# Setup service account and RBAC
setup_rbac() {
    print_step "Setting up service account and RBAC..."

    # Create namespace if it doesn't exist
    kubectl create namespace ${SPARK_NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -

    # Create service account
    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark-operator-spark
  namespace: ${SPARK_NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-operator-spark-role
  namespace: ${SPARK_NAMESPACE}
rules:
- apiGroups: [""]
  resources: ["pods", "services", "configmaps"]
  verbs: ["create", "get", "list", "watch", "update", "patch", "delete"]
- apiGroups: [""]
  resources: ["pods/log"]
  verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-operator-spark-rolebinding
  namespace: ${SPARK_NAMESPACE}
subjects:
- kind: ServiceAccount
  name: spark-operator-spark
  namespace: ${SPARK_NAMESPACE}
roleRef:
  kind: Role
  name: spark-operator-spark-role
  apiGroup: rbac.authorization.k8s.io
EOF

    print_step "RBAC setup completed"
}

# Verify installation
verify_installation() {
    print_step "Verifying installation..."

    echo "Waiting for Spark Operator to be ready..."
    kubectl wait --for=condition=ready pod \
        -l app.kubernetes.io/name=spark-operator \
        -n ${OPERATOR_NAMESPACE} \
        --timeout=300s

    echo ""
    print_step "Installation verified successfully!"
    echo ""
    echo "Cluster Information:"
    echo "  Cluster Name: ${CLUSTER_NAME}"
    echo "  Operator Namespace: ${OPERATOR_NAMESPACE}"
    echo "  Spark Namespace: ${SPARK_NAMESPACE}"
    echo ""
    echo "Next steps:"
    echo "  1. Set kubectl context: kubectl config use-context kind-${CLUSTER_NAME}"
    echo "  2. Run tests: python examples/spark/test_spark_client_integration.py"
    echo "  3. Delete cluster: kind delete cluster --name ${CLUSTER_NAME}"
}

# Main
main() {
    echo "========================================"
    echo " Spark Operator Test Environment Setup"
    echo "========================================"
    echo ""

    check_prerequisites
    create_cluster
    kind export kubeconfig --name spark-test
    # Try Helm first, fallback to kubectl
    if command -v helm &> /dev/null; then
        install_spark_operator_helm
    else
        install_spark_operator_kubectl
    fi

    setup_rbac
    verify_installation

    print_step "Setup complete! 🎉"
}

# Run main function
main
