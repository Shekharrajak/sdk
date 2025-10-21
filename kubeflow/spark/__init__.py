"""Kubeflow Spark Client for managing Spark applications on Kubernetes.

This module provides a unified Python client for managing Apache Spark applications
on Kubernetes using different backends:

- **OperatorBackend**: Cloud-native backend using Kubeflow Spark Operator (recommended)
- **GatewayBackend**: REST API backend for managed Spark gateways

Quick Start:
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

For more examples, see the examples/ directory.
"""

# Import the new backend-based implementation
# Export backend configs for advanced usage
from kubeflow.spark.backends import (
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
    DeployMode,
    DynamicAllocation,
    GPUSpec,
    MonitoringSpec,
    PrometheusSpec,
    # Configuration Models
    RestartPolicy,
    RestartPolicyType,
    # Request & Response
    SparkApplicationRequest,
    SparkApplicationResponse,
    SparkUIConfiguration,
)

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
