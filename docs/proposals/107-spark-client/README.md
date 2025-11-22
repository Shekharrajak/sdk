# KEP-107: Spark Client SDK for Kubeflow

## Authors

- Shekhar Rajak - [@shekharrajak](https://github.com/shekharrajak)

## Summary

This proposal introduces a unified Spark client SDK for Kubeflow that enables data engineers, ML engineers, and data scientists to seamlessly submit, monitor, and manage Apache Spark applications on Kubernetes. Following the same architectural pattern as the Kubeflow Trainer client, the Spark client provides a cloud-native, backend-agnostic Python API for running distributed data processing and ML workloads using the Kubeflow Spark Operator.

Ref https://github.com/kubeflow/sdk/issues/107

## Motivation

Apache Spark is a critical component in modern ML pipelines for large-scale data processing, feature engineering, and distributed model training. However, running Spark on Kubernetes currently requires users to:

- Manually create and manage complex SparkApplication CRDs
- Understand Kubernetes resource management and scheduling
- Write YAML configurations with extensive boilerplate
- Separately handle application submission, monitoring, and log retrieval
- Manage different Spark deployment patterns (standalone, client/server mode)

This creates friction for data engineers and ML practitioners who want to focus on their data processing logic rather than infrastructure management.

### Goals

- Design a unified, Pythonic SDK for managing Spark applications on Kubernetes
- Support multiple backends (Kubernetes Operator, REST Gateway, Spark Connect) following the Trainer pattern
- Enable both batch job submission and interactive session-based workflows
- Enable seamless integration with Kubeflow ML pipelines and workflows
- Provide comprehensive monitoring, logging, and debugging capabilities
- Support production features: dynamic allocation, GPU scheduling, volumes, monitoring
- Support interactive data exploration and notebook-style workflows via Spark Connect
- Deliver excellent developer experience with minimal boilerplate

### Non-Goals

- Supporting Spark applications outside Kubernetes (local mode, standalone clusters)
- Managing Spark cluster infrastructure (operator installation, cluster setup)
- Replacing the Spark Operator or modifying its behavior
- Supporting legacy Spark versions (< 3.0.0)
- Implementing custom Spark schedulers or resource managers

## Proposal

We propose a **SparkClient** SDK that follows the same architectural pattern as the Kubeflow Trainer client, providing:

1. **Backend Abstraction**: Pluggable backends for different deployment scenarios
2. **Kubernetes-Native**: Direct CRD manipulation for cloud-native deployments
3. **Unified API**: Same interface regardless of backend
4. **Config-Based Selection**: Backend selection through typed configuration objects

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     SparkClient (User API)                  │
│  - submit_application(), create_session(), get_status()     │
└──────────────────────┬──────────────────────────────────────┘
                       │ delegates to
                       ▼
┌─────────────────────────────────────────────────────────────┐
│          SparkBackend (Abstract Base Class)                 │
│  - Defines standard methods all backends must implement     │
└──────────────────────┬──────────────────────────────────────┘
                       │ implemented by
           ┌───────────┴─────────────┬──────────────────┬────────────────┐
           ▼                         ▼                  ▼                ▼
┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│OperatorBackend   │   │  GatewayBackend  │   │ ConnectBackend   │   │  LocalBackend    │
│(Spark Operator   │   │  (REST Gateway)  │   │  (Spark Connect/ │   │  (Future)        │
│ CRDs on K8s)     │   │                  │   │   Interactive)   │   │                  │
└──────────────────┘   └──────────────────┘   └──────────────────┘   └──────────────────┘
     Batch Jobs            Batch Jobs            Sessions              Local Dev
```

## Design Details

### SparkClient API

The main client provides a simple, intuitive API for all Spark operations:

#### Basic Spark Application Submission

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

#### Data Processing with S3/Object Storage

```python
from kubeflow.spark import SparkClient, OperatorBackendConfig

config = OperatorBackendConfig(
    namespace="spark-jobs",
    enable_monitoring=True,
    enable_ui=True,
)
client = SparkClient(backend_config=config)

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
        "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
    },
    env_vars={
        "AWS_ACCESS_KEY_ID": "your-key",
        "AWS_SECRET_ACCESS_KEY": "your-secret",
    }
)
```

#### Advanced Features: Dynamic Allocation, Volumes, GPU

```python
from kubeflow.spark import SparkClient, OperatorBackendConfig

config = OperatorBackendConfig(namespace="default")
client = SparkClient(backend_config=config)

response = client.submit_application(
    app_name="advanced-job",
    main_application_file="local:///app/job.py",
    spark_version="4.0.0",
    driver_cores=2,
    driver_memory="4g",
    executor_cores=2,
    executor_memory="8g",

    # Enable dynamic allocation (auto-scaling)
    enable_dynamic_allocation=True,
    initial_executors=2,
    min_executors=1,
    max_executors=10,

    # Configure volumes for data access
    volumes=[{
        "name": "data-volume",
        "persistentVolumeClaim": {"claimName": "my-pvc"}
    }],
    driver_volume_mounts=[{
        "name": "data-volume",
        "mountPath": "/data"
    }],

    # GPU configuration
    driver_gpu={"name": "nvidia.com/gpu", "quantity": 1},
    executor_gpu={"name": "nvidia.com/gpu", "quantity": 2},

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

### Backend Configurations

#### OperatorBackendConfig

```python
from kubeflow.spark.backends import OperatorBackendConfig

config = OperatorBackendConfig(
    namespace="default",                     # K8s namespace
    context=None,                            # kubeconfig context
    service_account="spark-operator-spark",  # Service account
    image_pull_policy="IfNotPresent",        # Image pull policy
    default_spark_image="gcr.io/spark-operator/spark-py",
    enable_monitoring=True,                  # Prometheus monitoring
    enable_ui=True,                          # Spark UI
    timeout=60,                              # API timeout (seconds)
)
```

#### GatewayBackendConfig

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

### Core Components

#### 1. SparkBackend (Abstract Base)

Defines the interface all backends must implement:

```python
class SparkBackend(abc.ABC):
    @abc.abstractmethod
    def submit_application(...) -> SparkApplicationResponse

    @abc.abstractmethod
    def get_status(submission_id: str) -> ApplicationStatus

    @abc.abstractmethod
    def wait_for_completion(...) -> ApplicationStatus

    @abc.abstractmethod
    def get_logs(...) -> Iterator[str]

    @abc.abstractmethod
    def list_applications(...) -> List[ApplicationStatus]

    @abc.abstractmethod
    def delete_application(submission_id: str) -> Dict
```

#### 2. OperatorBackend (Kubernetes CRD)

Implements the backend using Spark Operator CRDs:

- Creates `SparkApplication` CRDs with full configuration
- Manages lifecycle through Kubernetes API
- Supports all Spark Operator features (dynamic allocation, monitoring, GPU)
- Provides log streaming from driver and executor pods
- Enables Spark UI access through services/ingress

#### 3. GatewayBackend (REST API)

Implements the backend for managed Spark gateways:

- Submits applications via REST endpoints
- Compatible with Apache Livy and similar gateways
- Supports basic auth, token auth, and custom authentication
- Provides session-based and batch job execution


#### 3. ConnectBackend (Spark Connect / Interactive Sessions)

Implements session-oriented backend using Spark Connect protocol (gRPC):

**Key Features**:
- Remote connectivity to existing Spark clusters via Spark Connect (gRPC)
- Session-based interactive workloads (exploratory analysis, notebooks)
- Full PySpark DataFrame API support
- Artifact upload (JARs, Python files)
- SSL/TLS and Bearer token authentication

**Architecture**:
```python
ConnectBackend → ManagedSparkSession → Native PySpark SparkSession
```

**Use Cases**:
- Interactive data exploration and analysis
- Notebook-style workflows (Jupyter, IPython)
- Iterative development and testing
- Connecting to remote managed Spark clusters

**Configuration**:
```python
from kubeflow.spark import SparkClient, ConnectBackendConfig

config = ConnectBackendConfig(
    connect_url="sc://spark-cluster.default.svc:15002",
    token="bearer-token",  # Optional authentication
    use_ssl=True,          # SSL/TLS for secure communication
)
client = SparkClient(backend_config=config)

# Create interactive session
session = client.create_session(app_name="data-analysis")

# Use standard PySpark API
df = session.sql("SELECT * FROM sales WHERE date >= '2024-01-01'")
result = df.groupBy("product").sum("amount").collect()

# Upload artifacts
session.upload_artifacts("/path/to/lib.jar")
session.upload_artifacts("/path/to/package.zip", pyfile=True)

# Get session metrics
metrics = session.get_metrics()
print(f"Queries executed: {metrics.queries_executed}")

# Cleanup
session.close()
```

**Difference from Batch Backends**:
- **Session-oriented**: Long-lived connections vs. one-time job submission
- **Interactive**: Real-time query execution vs. batch processing
- **State management**: Maintains session state (temp views, UDFs)
- **API surface**: Session methods (create_session, close_session) vs. batch methods (submit_application, wait_for_completion)

**ManagedSparkSession API**:
```python
# Standard PySpark DataFrame API
df = session.sql("SELECT * FROM table")
df = session.read.parquet("s3a://bucket/data/")
df = session.table("database.table")
df = session.range(1000)

# Kubeflow extensions
session.upload_artifacts(*paths, pyfile=True)
session.export_to_pipeline_artifact(df, "/outputs/data.parquet")
metrics = session.get_metrics()
cloned = session.clone()  # Clone with shared state

# Context manager support
with client.create_session("my-app") as session:
    df = session.sql("SELECT * FROM data")
    # Auto-cleanup on exit
```

### Spark Connect Implementation Details

#### Connection URL Format

Spark Connect uses URL-based configuration:

```
sc://host:port/;param1=value;param2=value
```

**Examples**:
```python
# Kubernetes service
"sc://spark-cluster.default.svc.cluster.local:15002"

# With authentication
"sc://spark-cluster:15002/;token=bearer-token"

# Port-forwarded local
"sc://localhost:15002"
```

#### Session Lifecycle

```
1. Client Configuration → ConnectBackend initialized
2. create_session() → gRPC connection established
3. Session active → Execute queries, upload artifacts
4. close(release=True) → Resources released on server
5. Session closed → Can no longer execute operations
```

#### Backend Selection Strategy

| Backend | Use Case | Session Type | Lifecycle |
|---------|----------|--------------|-----------|
| **OperatorBackend** | Batch jobs, scheduled pipelines | One-time submission | Submit → Monitor → Complete/Fail |
| **GatewayBackend** | REST API integration, managed platforms | Batch or session | Submit via HTTP → Poll status |
| **ConnectBackend** | Interactive analysis, notebooks | Long-lived session | Create → Query → Close |

**When to use ConnectBackend**:
- Exploratory data analysis with immediate feedback
- Jupyter/IPython notebook workflows
- Interactive debugging and development
- Connecting to existing remote Spark clusters
- Requires Spark 3.4+ with Connect support

**When to use OperatorBackend**:
- Production batch ETL pipelines
- Scheduled data processing jobs
- CI/CD integration with Kubeflow Pipelines
- Full control over Spark cluster resources (driver, executors)
- Dynamic allocation and auto-scaling needs

### Example: Interactive Data Exploration

```python
from kubeflow.spark import SparkClient, ConnectBackendConfig

# Setup
config = ConnectBackendConfig(
    connect_url="sc://spark-connect-server.spark.svc:15002",
    use_ssl=True,
)
client = SparkClient(backend_config=config)

# Create session
with client.create_session(app_name="explore-sales") as session:
    # Load data
    sales_df = session.read.parquet("s3a://data/sales/")

    # Interactive analysis
    by_product = sales_df.groupBy("product").sum("revenue")
    top_products = by_product.orderBy("sum(revenue)", ascending=False).limit(10)

    # View results
    for row in top_products.collect():
        print(f"{row.product}: ${row['sum(revenue)']:,.2f}")

    # Export for pipeline
    session.export_to_pipeline_artifact(
        top_products,
        "/outputs/top_products.parquet"
    )

    # Check metrics
    metrics = session.get_metrics()
    print(f"Total queries: {metrics.queries_executed}")
```

### Example: Notebook Workflow

```python
# Cell 1: Setup
from kubeflow.spark import SparkClient, ConnectBackendConfig

config = ConnectBackendConfig(connect_url="sc://localhost:15002")
client = SparkClient(backend_config=config)
session = client.create_session("notebook-analysis")

# Cell 2: Load and explore
df = session.sql("SELECT * FROM customers LIMIT 100")
df.show()

# Cell 3: Feature engineering
features = df.select("customer_id", "age", "spend_total")
features = features.withColumn("spend_per_year", features.spend_total / features.age)

# Cell 4: Analysis
summary = features.describe()
summary.show()

# Cell 5: Cleanup
session.close()
```

### Integration with Other Backends

Combined workflow example:

```python
from kubeflow.spark import SparkClient, ConnectBackendConfig, OperatorBackendConfig

# Step 1: Interactive development with ConnectBackend
connect_config = ConnectBackendConfig(connect_url="sc://dev-cluster:15002")
dev_client = SparkClient(backend_config=connect_config)

with dev_client.create_session("dev") as session:
    # Test and validate query
    test_df = session.sql("SELECT * FROM data LIMIT 1000")
    test_df.show()
    # Iterate and refine...

# Step 2: Production batch job with OperatorBackend
prod_config = OperatorBackendConfig(namespace="production")
prod_client = SparkClient(backend_config=prod_config)

# Submit validated job at scale
response = prod_client.submit_application(
    app_name="production-etl",
    main_application_file="s3a://jobs/etl_pipeline.py",
    driver_cores=4,
    driver_memory="16g",
    executor_cores=4,
    executor_memory="32g",
    num_executors=50,
)
```
#### 4. Data Models

**SparkApplicationResponse**:
```python
@dataclass
class SparkApplicationResponse:
    submission_id: str      # Unique submission ID
    status: str             # Initial status
    message: Optional[str]  # Optional message
    timestamp: str          # Submission timestamp
```

**ApplicationStatus**:
```python
@dataclass
class ApplicationStatus:
    submission_id: str
    state: ApplicationState  # Enum: NEW, RUNNING, COMPLETED, FAILED, etc.
    app_id: Optional[str]    # Spark application ID
    driver_info: Optional[Dict]
    executor_info: Optional[Dict]
    submission_time: Optional[str]
    completion_time: Optional[str]
    message: Optional[str]
```

### User Stories

#### Story 1: Data Engineer Running ETL Pipeline

**As a** data engineer,
**I want to** submit daily ETL jobs using Spark with minimal configuration,
**So that** I can focus on data transformation logic rather than infrastructure.

```python
from kubeflow.spark import SparkClient

client = SparkClient()

# Simple ETL job submission
response = client.submit_application(
    app_name="daily-etl",
    main_application_file="s3a://data/etl/process_orders.py",
    driver_cores=2,
    driver_memory="4g",
    executor_cores=4,
    executor_memory="8g",
    num_executors=10,
    arguments=["--date", "2025-01-15"],
)

# Wait and validate
status = client.wait_for_completion(response.submission_id)
if status.state != ApplicationState.COMPLETED:
    # Handle failure
    logs = list(client.get_logs(response.submission_id))
    raise Exception(f"ETL failed: {logs[-100:]}")
```

#### Story 2: ML Engineer Training Models with Feature Engineering

**As an** ML engineer,
**I want to** run large-scale feature engineering with Spark and automatically scale resources,
**So that** I can efficiently prepare data for model training.

```python
from kubeflow.spark import SparkClient, OperatorBackendConfig

config = OperatorBackendConfig(
    namespace="ml-jobs",
    enable_monitoring=True,  # Track metrics
)
client = SparkClient(backend_config=config)

response = client.submit_application(
    app_name="feature-engineering",
    main_application_file="s3a://ml/features/extract.py",

    # Dynamic allocation for cost optimization
    enable_dynamic_allocation=True,
    min_executors=5,
    max_executors=50,

    # Resource configuration
    driver_cores=4,
    driver_memory="16g",
    executor_cores=4,
    executor_memory="32g",

    # S3 configuration
    spark_conf={
        "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
        "spark.sql.adaptive.enabled": "true",
    },
)
```

#### Story 3: Data Scientist Exploring Data Interactively

**As a** data scientist,
**I want to** run interactive Spark sessions for data exploration,
**So that** I can quickly iterate on analysis without managing infrastructure.

```python
from kubeflow.spark import SparkClient

client = SparkClient()

# Submit interactive exploration job
response = client.submit_application(
    app_name="explore-dataset",
    main_application_file="local:///workspace/explore.py",
    driver_cores=2,
    driver_memory="8g",
    executor_cores=2,
    executor_memory="16g",
    num_executors=5,

    # Mount data directory
    volumes=[{"name": "data", "hostPath": {"path": "/data"}}],
    driver_volume_mounts=[{"name": "data", "mountPath": "/data"}],
)

# Stream logs in real-time
for line in client.get_logs(response.submission_id, follow=True):
    print(line)
```

#### Story 4: Platform Engineer Managing Multiple Spark Jobs

**As a** platform engineer,
**I want to** list, monitor, and manage all Spark applications across namespaces,
**So that** I can ensure efficient resource utilization and troubleshoot issues.

```python
from kubeflow.spark import SparkClient

client = SparkClient()

# List all applications
apps = client.list_applications(
    namespace="all",
    labels={"team": "data-platform"}
)

# Monitor resource usage
for app in apps:
    status = client.get_status(app.submission_id)
    print(f"{app.submission_id}: {status.state}")
    print(f"  Executors: {status.executor_info}")

    # Cleanup failed jobs
    if status.state == ApplicationState.FAILED:
        client.delete_application(app.submission_id)
```

### Impact Analysis

#### Impact on Data Engineers

**Benefits**:
- **Simplified submission**: Single Python API for all operations
- **Better debugging**: Built-in log streaming and status monitoring
- **Production-ready**: Automatic retry policies, error handling

**Example**: Before (YAML + kubectl)
```yaml
# 100+ lines of YAML configuration
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-job
spec:
  type: Python
  mode: cluster
  # ... 90 more lines
```

After (SDK):
```python
# 10 lines of Python
client.submit_application(
    app_name="spark-job",
    main_application_file="s3a://data/job.py",
    driver_cores=2,
    driver_memory="4g",
    # ... clean, typed API
)
```

#### Impact on ML Engineers

**Benefits**:
- **Seamless integration**: Works with Kubeflow Pipelines and Trainer
- **Feature engineering at scale**: Easy distributed data processing
- **GPU support**: First-class GPU scheduling for Spark ML
- **Monitoring**: Built-in Prometheus metrics integration

**Use Cases**:
- Large-scale feature extraction and transformation
- Distributed hyperparameter tuning with SparkML
- Data validation and quality checks in pipelines
- Model serving with batch inference

#### Impact on Data Scientists

**Benefits**:
- **Focus on logic**: No infrastructure knowledge required
- **Quick iteration**: Fast submission and monitoring
- **Interactive exploration**: Support for notebook-style workflows
- **Reproducibility**: Version-controlled configurations

**Example Workflow**:
```python
# In Jupyter notebook
from kubeflow.spark import SparkClient

client = SparkClient()

# Iterate on data exploration
for sample_size in [1000, 10000, 100000]:
    response = client.submit_application(
        app_name=f"explore-{sample_size}",
        main_application_file="local:///workspace/analyze.py",
        arguments=[f"--sample-size={sample_size}"],
    )
    status = client.wait_for_completion(response.submission_id)
    # Analyze results
```

#### Impact on Operations and Maintenance

**Operational Benefits**:
- **Standardization**: Consistent API across all Spark deployments
- **Observability**: Built-in monitoring and logging
- **Resource management**: Dynamic allocation reduces waste
- **Cost optimization**: Auto-scaling executors based on workload

**Maintenance Benefits**:
- **Reduced complexity**: Backend abstraction simplifies upgrades
- **Version management**: Easy Spark version switching
- **Debugging**: Comprehensive validation and error messages
- **Testing**: Unit and integration tests included

**SRE Metrics Impact**:
- **MTTR (Mean Time To Recovery)**: reduction via better error messages
- **Deployment frequency**: simplified API
- **Resource utilization**:  improvement via dynamic allocation
- **Failure rate**:reduction via built-in validation

### Modularity and Extensibility

#### Backend Modularity

The SDK is designed for easy extension with new backends:

```python
# Custom backend implementation
from kubeflow.spark.backends.base import SparkBackend

class CustomBackend(SparkBackend):
    def submit_application(self, ...):
        # Custom submission logic
        pass

    def get_status(self, submission_id):
        # Custom status retrieval
        pass
```

#### Plugin System (Future)

```python
# Custom validators
from kubeflow.spark.validation import SparkApplicationValidator

class SecurityValidator(SparkApplicationValidator):
    def validate(self, spec):
        # Custom security checks
        pass

client = SparkClient(validators=[SecurityValidator()])
```

#### Configuration Hierarchy

```python
# Global defaults
SparkClient.set_defaults(
    driver_memory="4g",
    executor_memory="8g",
)

# Backend-specific overrides
config = OperatorBackendConfig(
    namespace="prod",
    enable_monitoring=True,
)

# Per-job overrides
client.submit_application(
    driver_memory="16g",  # Override default
    ...
)
```

### Integration with Kubeflow Ecosystem

#### Integration with Trainer

```python
# Combined ML pipeline
from kubeflow.trainer import TrainerClient
from kubeflow.spark import SparkClient

# Step 1: Feature engineering with Spark
spark_client = SparkClient()
features = spark_client.submit_application(
    app_name="extract-features",
    main_application_file="s3a://ml/extract.py",
)
spark_client.wait_for_completion(features.submission_id)

# Step 2: Model training with Trainer
trainer_client = TrainerClient()
trainer_client.train(
    name="train-model",
    func=train_func,
    num_nodes=4,
)
```

#### Integration with Pipelines

```python
# Kubeflow Pipeline component
from kfp import dsl
from kubeflow.spark import SparkClient

@dsl.component
def spark_preprocessing(data_path: str) -> str:
    client = SparkClient()
    response = client.submit_application(
        app_name="preprocess",
        main_application_file=f"s3a://{data_path}/preprocess.py",
    )
    status = client.wait_for_completion(response.submission_id)
    return status.app_id
```

### Comparison with Trainer Client

| Aspect | Trainer Client | Spark Client |
|--------|---------------|--------------|
| **CRD** | TrainJob | SparkApplication |
| **Operator** | Training Operator | Spark Operator |
| **Backends** | Kubernetes, LocalProcess, Container | Operator, Gateway, Connect |
| **Primary Use Case** | Distributed model training | Data processing, feature engineering, interactive analysis |
| **Distributed Pattern** | Multi-node training (MPI, PyTorch) | Multi-executor Spark (driver + executors) |
| **Dynamic Scaling** | Fixed replicas | Dynamic allocation (auto-scaling) |
| **Storage** | PVCs, object storage via initializers | HDFS, S3, object storage natively |
| **Monitoring** | Job logs, status | Spark UI, metrics, executor logs |
| **API Style** | train(), list_jobs() | submit_application(), create_session() |
| **Workload Type** | Batch training jobs | Batch jobs + interactive sessions |
| **Session Support** | No | Yes (via Connect backend) |

Both clients share:
- Backend abstraction for flexibility
- Kubernetes-native CRD management
- Status monitoring with polling
- Log streaming capabilities
- Type-safe, validated configurations

### SparkApplication CRD Example

The SDK generates SparkApplication CRDs like this:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: dataframe-analysis
  namespace: spark-jobs
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: gcr.io/spark-operator/spark-py:4.0.0
  imagePullPolicy: IfNotPresent
  mainApplicationFile: s3a://my-bucket/jobs/analysis.py
  sparkVersion: "4.0.0"

  driver:
    cores: 2
    coreLimit: "2000m"
    memory: "4g"
    serviceAccount: spark-operator-spark

  executor:
    cores: 2
    instances: 5
    memory: "8g"

  dynamicAllocation:
    enabled: true
    initialExecutors: 2
    minExecutors: 1
    maxExecutors: 10
    shuffleTrackingEnabled: true

  sparkConf:
    "spark.sql.shuffle.partitions": "200"
    "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com"

  monitoring:
    exposeDriverMetrics: true
    exposeExecutorMetrics: true
    prometheus:
      jmxExporterJar: "/prometheus/jmx_prometheus_javaagent.jar"
```



## Testing Strategy

### Unit Tests

- Backend configuration validation
- Model serialization/deserialization
- CRD generation from API parameters
- Error handling and retry logic
- Validation rules (resource limits, naming)

### Integration Tests

Comprehensive tests in `examples/spark/test_spark_client_integration.py`:

```python
def test_basic_submission():
    """Test basic Spark application submission"""
    client = SparkClient()
    response = client.submit_application(
        app_name="test-pi",
        main_application_file="local:///opt/spark/examples/pi.py",
        driver_cores=1,
        driver_memory="512m",
    )
    assert response.submission_id
    status = client.wait_for_completion(response.submission_id)
    assert status.state == ApplicationState.COMPLETED

def test_dynamic_allocation():
    """Test dynamic executor allocation"""
    # Test auto-scaling behavior

def test_s3_integration():
    """Test S3/object storage integration"""
    # Test with MinIO/S3
```


### Test Environment Setup

```bash
# Automated setup script
cd examples/spark
./setup_test_environment.sh

# This script:
# 1. Creates Kind cluster
# 2. Installs Spark Operator
# 3. Configures RBAC and service accounts
# 4. Sets up MinIO for S3 testing
# 5. Runs validation tests
```

## Migration and Adoption Path


### For Existing Spark Users

Migration from manual CRD management:

**Before**:
```bash
# Create YAML file
cat > spark-job.yaml << EOF
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
...
EOF

# Submit
kubectl apply -f spark-job.yaml

# Monitor
kubectl get sparkapplication -w

# Get logs
kubectl logs spark-job-driver

# Cleanup
kubectl delete sparkapplication spark-job
```

**After**:
```python
from kubeflow.spark import SparkClient

client = SparkClient()

# All operations in one place
response = client.submit_application(...)
status = client.wait_for_completion(response.submission_id)
logs = list(client.get_logs(response.submission_id))
client.delete_application(response.submission_id)
```


## Conclusion

The Spark Client SDK provides a production-ready, user-friendly Python interface for managing Apache Spark applications on Kubernetes. By following the proven Trainer pattern and focusing on developer experience, modularity, and operational excellence, this SDK enables data engineers, ML engineers, and data scientists to efficiently leverage Spark for large-scale data processing within the Kubeflow ecosystem.

