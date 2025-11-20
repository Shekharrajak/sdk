# Kubeflow Spark Client

Cloud-native Python client for managing Apache Spark applications on Kubernetes using the Kubeflow Spark Operator.

## Overview

The Kubeflow Spark Client provides a unified, Pythonic interface for submitting, monitoring, and managing Spark applications on Kubernetes. It follows the same architecture pattern as the Kubeflow Trainer client, supporting multiple backends for different deployment scenarios.

### Key Features

- **Cloud-Native Architecture**: Direct integration with Kubeflow Spark Operator CRDs
- **Multiple Backends**: Operator (K8s-native) and Gateway (REST API) backends
- **Dynamic Resource Allocation**: Automatic executor scaling based on workload
- **Comprehensive Monitoring**: Prometheus metrics and Spark UI integration
- **Production-Ready**: Error handling, retries, and comprehensive logging
- **Easy to Use**: Simple, intuitive API following Python best practices

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     SparkClient (User API)                  │
│  - submit_application(), get_status(), get_logs(), delete() │
└──────────────────────┬──────────────────────────────────────┘
                       │ delegates to
                       ▼
┌─────────────────────────────────────────────────────────────┐
│          SparkBackend (Abstract Base Class)                 │
│  - Defines standard methods all backends must implement     │
└──────────────────────┬──────────────────────────────────────┘
                       │ implemented by
           ┌───────────┴────────────┬──────────────────┐
           ▼                        ▼                  ▼
┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│OperatorBackend   │   │  GatewayBackend  │   │  LocalBackend    │
│(Spark Operator   │   │  (REST Gateway)  │   │  (Future)        │
│ CRDs on K8s)     │   │                  │   │                  │
└──────────────────┘   └──────────────────┘   └──────────────────┘
```

### Design Principles

The Spark client follows the same architectural pattern as the Kubeflow Trainer client:

1. **Backend Abstraction**: Pluggable backends allow running Spark on different platforms
2. **Config-Based Selection**: Backend selection through typed configuration objects
3. **Kubernetes-Native**: Direct CRD manipulation for cloud-native deployments
4. **Unified API**: Same interface regardless of backend

## Installation

```bash
# Install from PyPI (when released)
pip install kubeflow

# Or install from source
cd sdk
pip install -e .
```

### Prerequisites

For **OperatorBackend** (recommended):
- Kubernetes cluster (1.16+)
- Kubeflow Spark Operator installed
- kubectl configured with proper context
- Service account with SparkApplication permissions

For **GatewayBackend**:
- Access to a Batch Processing Gateway
- API credentials (if required)

## Quick Start

### Basic Example

```python
from kubeflow.spark import SparkClient

# Create client (uses Operator backend by default)
client = SparkClient()

# Submit a Spark application
response = client.submit_application(
    app_name="spark-pi",
    main_application_file="local:///opt/spark/examples/src/main/python/pi.py",
    driver_cores=1,
    driver_memory="512m",
    executor_cores=1,
    executor_memory="512m",
    num_executors=2
)

print(f"Submitted: {response.submission_id}")

# Wait for completion
status = client.wait_for_completion(response.submission_id)
print(f"Final state: {status.state}")

# Get logs
for line in client.get_logs(response.submission_id):
    print(line)
```

### DataFrame Operations

```python
from kubeflow.spark import SparkClient, OperatorBackendConfig

# Configure client
config = OperatorBackendConfig(
    namespace="spark-jobs",
    enable_monitoring=True,
    enable_ui=True,
)
client = SparkClient(backend_config=config)

# Submit DataFrame processing job
response = client.submit_application(
    app_name="dataframe-analysis",
    main_application_file="s3a://my-bucket/jobs/analysis.py",
    spark_version="4.0.0",
    driver_cores=2,
    driver_memory="4g",
    executor_cores=2,
    executor_memory="8g",
    num_executors=5,
    spark_conf={
        "spark.sql.shuffle.partitions": "200",
        "spark.executor.memoryOverhead": "1g",
    },
    env_vars={
        "AWS_ACCESS_KEY_ID": "your-key",
        "AWS_SECRET_ACCESS_KEY": "your-secret",
    }
)
```

### Advanced Features

```python
from kubeflow.spark import SparkClient, OperatorBackendConfig

config = OperatorBackendConfig(namespace="default")
client = SparkClient(backend_config=config)

# Submit with dynamic allocation and volumes
response = client.submit_application(
    app_name="advanced-job",
    main_application_file="local:///app/job.py",
    spark_version="4.0.0",
    driver_cores=2,
    driver_memory="4g",
    executor_cores=2,
    executor_memory="8g",
    num_executors=3,

    # Enable dynamic allocation
    enable_dynamic_allocation=True,
    initial_executors=2,
    min_executors=1,
    max_executors=10,

    # Configure volumes
    volumes=[{
        "name": "data-volume",
        "persistentVolumeClaim": {"claimName": "my-pvc"}
    }],
    driver_volume_mounts=[{
        "name": "data-volume",
        "mountPath": "/data"
    }],
    executor_volume_mounts=[{
        "name": "data-volume",
        "mountPath": "/data"
    }],

    # Node selector and tolerations
    node_selector={"node-type": "compute"},
    tolerations=[{
        "key": "spark",
        "operator": "Equal",
        "value": "true",
        "effect": "NoSchedule"
    }],
)
```

## API Reference

### SparkClient

Main client for managing Spark applications.

#### Methods

- **submit_application(...)** → SparkApplicationResponse
  - Submit a new Spark application
  - Returns submission ID and status

- **get_status(submission_id)** → ApplicationStatus
  - Get current status of an application
  - Returns state, app ID, executor info, etc.

- **wait_for_completion(submission_id, timeout, polling_interval)** → ApplicationStatus
  - Block until application completes
  - Returns final status

- **get_logs(submission_id, executor_id=None, follow=False)** → Iterator[str]
  - Stream application logs
  - Can get driver or specific executor logs

- **list_applications(namespace=None, labels=None)** → List[ApplicationStatus]
  - List applications (OperatorBackend only)
  - Optional filtering by namespace and labels

- **delete_application(submission_id)** → Dict
  - Delete an application
  - Stops running application and cleans up resources

### Backend Configuration

#### OperatorBackendConfig

Configuration for Kubernetes Spark Operator backend.

```python
from kubeflow.spark.backends import OperatorBackendConfig

config = OperatorBackendConfig(
    namespace="default",                    # K8s namespace
    context=None,                          # kubeconfig context
    service_account="spark-operator-spark", # Service account
    image_pull_policy="IfNotPresent",      # Image pull policy
    default_spark_image="docker.io/library/spark",  # Default image
    enable_monitoring=True,                 # Prometheus monitoring
    enable_ui=True,                        # Spark UI
    timeout=60,                            # API timeout (seconds)
)
```

#### GatewayBackendConfig

Configuration for REST Gateway backend.

```python
from kubeflow.spark.backends import GatewayBackendConfig
from kubeflow.spark.config import AuthMethod

config = GatewayBackendConfig(
    gateway_url="http://gateway:8080",
    user="myuser",
    password="mypassword",
    auth_method=AuthMethod.BASIC,
    timeout=30,
    verify_ssl=True,
)
```

## Examples

The `examples/spark/` directory contains comprehensive examples:

- **example_spark_pi.py**: Basic Spark Pi calculation
- **spark_dataframe_operations.py**: DataFrame transformations and SQL
- **example_dataframe_job.py**: Submitting custom DataFrame jobs
- **example_advanced_features.py**: Dynamic allocation, volumes, monitoring

Run examples:

```bash
cd examples/spark

# Basic example
python example_spark_pi.py

# DataFrame operations
python example_dataframe_job.py

# Advanced features
python example_advanced_features.py
```

## Testing

### Setup Test Environment

Use the provided script to set up a Kind cluster with Spark Operator:

```bash
cd examples/spark
./setup_test_environment.sh
```

This will:
1. Create a Kind cluster
2. Install Spark Operator
3. Configure RBAC and service accounts
4. Verify the installation

### Run Integration Tests

```bash
# After setting up the test environment
python test_spark_client_integration.py
```

The integration tests cover:
- Application submission
- Status monitoring
- Log retrieval
- Dynamic allocation
- Application deletion

### Cleanup

```bash
kind delete cluster --name spark-test
```

## Comparison with Trainer Client

The Spark client follows the same architectural patterns as the Trainer client:

| Aspect | Trainer Client | Spark Client |
|--------|---------------|--------------|
| **CRD** | TrainJob | SparkApplication |
| **Operator** | Training Operator | Spark Operator |
| **Backends** | Kubernetes, LocalProcess, Container | Operator, Gateway |
| **Config Pattern** | Backend-specific configs | Backend-specific configs |
| **API Style** | train(), list_jobs(), get_job() | submit_application(), list_applications(), get_status() |
| **Distributed** | Multi-node training | Multi-executor Spark |

Both clients provide:
- Backend abstraction for flexibility
- Kubernetes-native CRD management
- Status monitoring with polling
- Log streaming capabilities
- Context manager support

## Monitoring and Debugging

### Access Spark UI

**Port Forward to Spark UI:**
```bash
kubectl port-forward -n default svc/spark-connect 4040:4040
```

Open in browser: http://localhost:4040

**Navigate Spark UI Tabs:**
- **Jobs Tab**: View all jobs, click job to see stages and tasks
- **Stages Tab**: View stage details, task metrics, and task-level stdout/stderr
- **Executors Tab**: Click stdout/stderr links for each executor to view logs
- **SQL Tab**: View DataFrame query execution plans and metrics
- **Environment Tab**: Check Spark configuration and environment variables

### Debug DataFrame Queries

**View Query Execution Plans:**
```python
# In Spark UI -> SQL Tab
# Shows:
# - Physical and logical plans
# - Query execution DAG
# - Shuffle and I/O metrics
# - Task execution timeline
```

**Check Query Performance:**
- Click on query in SQL Tab to see detailed metrics
- View stage execution time and data shuffle
- Identify bottlenecks in task distribution
- Check partition skew and executor utilization

### View Application Logs

**Using kubectl:**
```bash
# Spark Connect driver logs
kubectl logs -l app=spark-connect -n default -f

# SparkApplication driver logs
kubectl logs <app-name>-driver -n default

# Executor logs
kubectl logs <app-name>-exec-1 -n default
```

**Using SparkClient:**
```python
# Stream logs from driver
for line in client.get_logs(submission_id):
    print(line)

# Get specific executor logs
for line in client.get_logs(submission_id, executor_id="1"):
    print(line)
```

**Filter logs by level:**
```bash
kubectl logs -l app=spark-connect -n default | grep ERROR
kubectl logs -l app=spark-connect -n default | grep -E "WARN|ERROR"
```

## Troubleshooting

### Common Issues

**1. SparkApplication not being created**

Check Spark Operator is running:
```bash
kubectl get pods -n spark-operator
```

**2. Pods stuck in Pending**

Check service account and RBAC:
```bash
kubectl get serviceaccount spark-operator-spark
kubectl describe role spark-operator-spark-role
```

**3. Cannot get logs**

Ensure pod is running:
```bash
kubectl get pods -l sparkoperator.k8s.io/app-name=<app-name>
```

**4. Permission denied**

Verify service account has correct permissions:
```bash
kubectl auth can-i create pods --as=system:serviceaccount:default:spark-operator-spark
```

### Debug Mode

Enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

from kubeflow.spark import SparkClient
client = SparkClient()
```

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

Apache License 2.0

## References

- [Kubeflow Spark Operator](https://github.com/kubeflow/spark-operator)
- [Apache Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
- [Kubeflow Trainer Client](https://github.com/kubeflow/trainer)

## Support

For issues and questions:
- GitHub Issues: [kubeflow/sdk](https://github.com/kubeflow/sdk/issues)
- Slack: [#kubeflow-spark](https://kubeflow.slack.com/archives/kubeflow-spark)
- Mailing List: kubeflow-discuss@googlegroups.com
