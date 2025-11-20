"""Kubeflow Spark Client for managing Spark applications on Kubernetes.

This module provides a unified Python client for managing Apache Spark applications
on Kubernetes using different backends:

- **OperatorBackend**: Cloud-native backend using Kubeflow Spark Operator (recommended for batch jobs)
- **GatewayBackend**: REST API backend for managed Spark gateways
- **ConnectBackend**: Spark Connect backend for remote interactive sessions

Quick Start (Batch Jobs):
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
        num_executors=2,
    )

    # Wait for completion
    status = client.wait_for_completion(response.submission_id)
    print(f"Application state: {status.state}")
    ```

Quick Start (Interactive Sessions with Spark Connect):
    ```python
    from kubeflow.spark import SparkClient, ConnectBackendConfig

    # Connect to existing Spark cluster
    config = ConnectBackendConfig(connect_url="sc://spark-cluster:15002")
    client = SparkClient(backend_config=config)

    # Create interactive session
    session = client.create_session(app_name="data-analysis")

    # Use standard PySpark API
    df = session.sql("SELECT * FROM table")
    result = df.filter(df.status == "active").collect()

    # Cleanup
    session.close()
    ```

For more examples, see the examples/ directory.
"""

# Import the new backend-based implementation
# Export backend configs for advanced usage
from kubeflow.spark.backends import (
    ConnectBackend,
    ConnectBackendConfig,
    GatewayBackend,
    GatewayBackendConfig,
    OperatorBackend,
    OperatorBackendConfig,
    SparkBackend,
)
from kubeflow.spark.client import (
    SparkClient,
    create_gateway_client,
    create_operator_client,
)
from kubeflow.spark.models import (
    # States & Enums
    ApplicationState,
    # Status Models
    ApplicationStatus,
    BatchSchedulerConfig,
    ConnectBackendConfig,
    DeployMode,
    DynamicAllocation,
    GPUSpec,
    MonitoringSpec,
    PrometheusSpec,
    # Configuration Models
    RestartPolicy,
    RestartPolicyType,
    # Session Models (for Spark Connect)
    SessionInfo,
    SessionMetrics,
    # Request & Response
    SparkApplicationRequest,
    SparkApplicationResponse,
    SparkUIConfiguration,
)
from kubeflow.spark.session import ManagedSparkSession

# Export validation
from kubeflow.spark.validation import (
    SparkApplicationValidator,
    ValidationError,
    ValidationErrorType,
    ValidationResult,
    validate_spark_application,
)

__all__ = [
    # Main client
    "SparkClient",
    "create_operator_client",
    "create_gateway_client",
    # Backends
    "SparkBackend",
    "OperatorBackend",
    "OperatorBackendConfig",
    "GatewayBackend",
    "GatewayBackendConfig",
    "ConnectBackend",
    "ConnectBackendConfig",
    # Session Management (Spark Connect)
    "ManagedSparkSession",
    "SessionInfo",
    "SessionMetrics",
    # Request & Response Models
    "SparkApplicationRequest",
    "SparkApplicationResponse",
    "ApplicationStatus",
    # States & Enums
    "ApplicationState",
    "RestartPolicyType",
    "DeployMode",
    # Configuration Models
    "RestartPolicy",
    "GPUSpec",
    "DynamicAllocation",
    "BatchSchedulerConfig",
    "PrometheusSpec",
    "MonitoringSpec",
    "SparkUIConfiguration",
    # Validation
    "validate_spark_application",
    "SparkApplicationValidator",
    "ValidationResult",
    "ValidationError",
    "ValidationErrorType",
]

__version__ = "0.2.0"
