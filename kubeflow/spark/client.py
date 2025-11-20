# Copyright 2025 The Kubeflow Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Main Spark client for managing Spark applications."""

from collections.abc import Iterator
import logging
from typing import Any, Dict, List, Optional, Union

from kubeflow.spark.backends.base import SparkBackend
from kubeflow.spark.backends.connect import ConnectBackend, ConnectBackendConfig
from kubeflow.spark.backends.gateway import GatewayBackend, GatewayBackendConfig
from kubeflow.spark.backends.operator import OperatorBackend, OperatorBackendConfig
from kubeflow.spark.models import ApplicationStatus, SessionInfo, SparkApplicationResponse
from kubeflow.spark.session import ManagedSparkSession

logger = logging.getLogger(__name__)


class SparkClient:
    """Main client for managing Spark applications on Kubernetes.

    This client provides a unified interface for managing Spark applications
    using different backends:
    - **OperatorBackend**: Cloud-native K8s backend using Spark Operator CRDs
    - **GatewayBackend**: REST API backend for managed Spark gateways

    The client automatically selects the appropriate backend based on the
    provided configuration.

    Example with Operator Backend (recommended for cloud-native deployments):
        ```python
        from kubeflow.spark import SparkClient
        from kubeflow.spark.backends import OperatorBackendConfig

        # Initialize client with Operator backend
        config = OperatorBackendConfig(namespace="spark-jobs")
        client = SparkClient(backend_config=config)

        # Submit a Spark application
        response = client.submit_application(
            app_name="my-spark-job",
            main_application_file="local:///app/main.py",
            driver_cores=2,
            driver_memory="4g",
            executor_cores=2,
            executor_memory="8g",
            num_executors=3,
        )

        # Wait for completion
        status = client.wait_for_completion(response.submission_id)
        print(f"Application completed with state: {status.state}")

        # Get logs
        for line in client.get_logs(response.submission_id):
            print(line)
        ```

    Example with Gateway Backend:
        ```python
        from kubeflow.spark import SparkClient
        from kubeflow.spark.backends import GatewayBackendConfig

        config = GatewayBackendConfig(gateway_url="http://gateway:8080", user="myuser")
        client = SparkClient(backend_config=config)
        ```

    Quick Start (uses defaults):
        ```python
        from kubeflow.spark import SparkClient

        # Uses OperatorBackend with default namespace
        client = SparkClient()

        response = client.submit_application(
            app_name="spark-pi", main_application_file="local:///opt/spark/examples/src/main/python/pi.py"
        )
        ```
    """

    def __init__(
        self,
        backend_config: Union[
            OperatorBackendConfig, GatewayBackendConfig, ConnectBackendConfig, None
        ] = None,
    ):
        """Initialize Spark client with specified backend.

        Args:
            backend_config: Backend configuration. Either:
                          - OperatorBackendConfig: Kubernetes with Spark Operator
                          - GatewayBackendConfig: REST API gateway
                          - ConnectBackendConfig: Spark Connect for remote clusters
                          Defaults to OperatorBackendConfig.

        Raises:
            ValueError: Invalid backend configuration
        """
        # Set default backend config
        if backend_config is None:
            backend_config = OperatorBackendConfig()

        # Initialize appropriate backend
        if isinstance(backend_config, OperatorBackendConfig):
            self.backend: SparkBackend = OperatorBackend(backend_config)
        elif isinstance(backend_config, GatewayBackendConfig):
            self.backend = GatewayBackend(backend_config)
        elif isinstance(backend_config, ConnectBackendConfig):
            self.backend = ConnectBackend(backend_config)
        else:
            raise ValueError(f"Invalid backend config type: {type(backend_config)}")

        logger.info(f"Initialized SparkClient with {type(self.backend).__name__}")

    def submit_application(
        self,
        app_name: str,
        main_application_file: str,
        spark_version: str = "3.5.0",
        app_type: str = "Python",
        driver_cores: int = 1,
        driver_memory: str = "1g",
        executor_cores: int = 1,
        executor_memory: str = "1g",
        num_executors: int = 2,
        queue: Optional[str] = None,
        arguments: Optional[List[str]] = None,
        python_version: str = "3",
        spark_conf: Optional[Dict[str, str]] = None,
        hadoop_conf: Optional[Dict[str, str]] = None,
        env_vars: Optional[Dict[str, str]] = None,
        deps: Optional[Dict[str, List[str]]] = None,
        **kwargs: Any,
    ) -> SparkApplicationResponse:
        """Submit a Spark application.

        Args:
            app_name: Name of the application (must be DNS-compliant for K8s)
            main_application_file: Path to main application file
                - For local files in container: "local:///path/to/file.py"
                - For S3: "s3a://bucket/path/to/file.py"
                - For HDFS: "hdfs://namenode/path/to/file.py"
            spark_version: Spark version to use (e.g., "3.5.0", "4.0.0")
            app_type: Application type - "Python", "Scala", "Java", or "R"
            driver_cores: Number of CPU cores for driver
            driver_memory: Memory for driver (e.g., "1g", "512m", "4g")
            executor_cores: Number of CPU cores per executor
            executor_memory: Memory per executor (e.g., "1g", "2g", "8g")
            num_executors: Number of executor instances
            queue: Queue/namespace to submit to (overrides backend default)
            arguments: Application arguments as list of strings
            python_version: Python version for PySpark apps ("2" or "3")
            spark_conf: Spark configuration properties as dict
            hadoop_conf: Hadoop configuration properties as dict
            env_vars: Environment variables as dict
            deps: Dependencies dict with keys: "jars", "pyFiles", "files"
            **kwargs: Additional backend-specific parameters:
                - volumes: List of Kubernetes volumes
                - driver_volume_mounts: Driver volume mounts
                - executor_volume_mounts: Executor volume mounts
                - node_selector: Node selector dict
                - tolerations: Tolerations list
                - enable_dynamic_allocation: Enable dynamic executor allocation
                - min_executors: Minimum executors (for dynamic allocation)
                - max_executors: Maximum executors (for dynamic allocation)

        Returns:
            SparkApplicationResponse with submission details

        Raises:
            ValueError: If required parameters are invalid
            RuntimeError: If submission fails
            TimeoutError: If submission times out

        Example:
            ```python
            response = client.submit_application(
                app_name="spark-pi",
                main_application_file="local:///opt/spark/examples/src/main/python/pi.py",
                arguments=["1000"],
                driver_cores=1,
                driver_memory="512m",
                executor_cores=1,
                executor_memory="512m",
                num_executors=2,
                spark_conf={"spark.executor.instances": "2", "spark.kubernetes.driver.label.app": "spark-pi"},
            )
            print(f"Submitted with ID: {response.submission_id}")
            ```
        """
        return self.backend.submit_application(
            app_name=app_name,
            main_application_file=main_application_file,
            spark_version=spark_version,
            app_type=app_type,
            driver_cores=driver_cores,
            driver_memory=driver_memory,
            executor_cores=executor_cores,
            executor_memory=executor_memory,
            num_executors=num_executors,
            queue=queue,
            arguments=arguments,
            python_version=python_version,
            spark_conf=spark_conf,
            hadoop_conf=hadoop_conf,
            env_vars=env_vars,
            deps=deps,
            **kwargs,
        )

    def get_status(self, submission_id: str) -> ApplicationStatus:
        """Get current status of a Spark application.

        Args:
            submission_id: Submission ID (typically same as app_name for Operator backend)

        Returns:
            ApplicationStatus object with current state and metadata

        Raises:
            RuntimeError: If status check fails
            TimeoutError: If status check times out

        Example:
            ```python
            status = client.get_status("spark-pi")
            print(f"State: {status.state}")
            print(f"Spark App ID: {status.app_id}")
            print(f"Driver Info: {status.driver_info}")
            ```
        """
        return self.backend.get_status(submission_id)

    def delete_application(self, submission_id: str) -> Dict[str, Any]:
        """Delete a Spark application.

        This will stop the application if running and remove its resources.

        Args:
            submission_id: Submission ID to delete

        Returns:
            Dictionary with deletion response

        Raises:
            RuntimeError: If deletion fails
            TimeoutError: If deletion times out

        Example:
            ```python
            result = client.delete_application("spark-pi")
            print(f"Deleted: {result}")
            ```
        """
        return self.backend.delete_application(submission_id)

    def get_logs(
        self,
        submission_id: str,
        executor_id: Optional[str] = None,
        follow: bool = False,
    ) -> Iterator[str]:
        """Get application logs from driver or executor pods.

        Args:
            submission_id: Submission ID
            executor_id: Optional executor ID (e.g., "1", "2").
                        If None, returns driver logs
            follow: Whether to stream logs in real-time (only OperatorBackend)

        Yields:
            Log lines as strings

        Raises:
            RuntimeError: If log retrieval fails

        Example:
            ```python
            # Get driver logs
            for line in client.get_logs("spark-pi"):
                print(line)

            # Get specific executor logs
            for line in client.get_logs("spark-pi", executor_id="1"):
                print(line)

            # Stream logs in real-time
            for line in client.get_logs("spark-pi", follow=True):
                print(line)
            ```
        """
        return self.backend.get_logs(
            submission_id=submission_id,
            executor_id=executor_id,
            follow=follow,
        )

    def list_applications(
        self,
        namespace: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
    ) -> List[ApplicationStatus]:
        """List Spark applications.

        Note: Only available with OperatorBackend.

        Args:
            namespace: Optional namespace filter
            labels: Optional label filters (e.g., {"app": "spark-pi"})

        Returns:
            List of ApplicationStatus objects

        Raises:
            NotImplementedError: If backend doesn't support listing
            RuntimeError: If listing fails
            TimeoutError: If listing times out

        Example:
            ```python
            # List all applications in default namespace
            apps = client.list_applications()
            for app in apps:
                print(f"{app.app_name}: {app.state}")

            # List with label filter
            apps = client.list_applications(labels={"team": "data-science"})
            ```
        """
        return self.backend.list_applications(namespace=namespace, labels=labels)

    def wait_for_completion(
        self,
        submission_id: str,
        timeout: int = 3600,
        polling_interval: int = 10,
    ) -> ApplicationStatus:
        """Wait for Spark application to complete.

        Blocks until the application reaches a terminal state (COMPLETED or FAILED).

        Args:
            submission_id: Submission ID to monitor
            timeout: Maximum time to wait in seconds (default: 3600 = 1 hour)
            polling_interval: Polling interval in seconds (default: 10)

        Returns:
            Final ApplicationStatus

        Raises:
            TimeoutError: If application doesn't complete within timeout
            RuntimeError: If monitoring fails

        Example:
            ```python
            response = client.submit_application(...)

            # Wait for completion
            try:
                status = client.wait_for_completion(
                    response.submission_id,
                    timeout=1800,  # 30 minutes
                )
                if status.state == ApplicationState.COMPLETED:
                    print("Application succeeded!")
                else:
                    print(f"Application failed: {status.state}")
            except TimeoutError:
                print("Application timed out")
            ```
        """
        return self.backend.wait_for_completion(
            submission_id=submission_id,
            timeout=timeout,
            polling_interval=polling_interval,
        )

    def wait_for_pod_ready(
        self,
        submission_id: str,
        executor_id: Optional[str] = None,
        timeout: int = 300,
    ) -> bool:
        """Wait for driver or executor pod to be ready (Operator backend only).

        Args:
            submission_id: Name of the SparkApplication
            executor_id: Optional executor ID. If None, waits for driver pod
            timeout: Maximum time to wait in seconds

        Returns:
            True if pod becomes ready, False if timeout or not supported

        Example:
            ```python
            # Submit application
            response = client.submit_application(app_name="my-job", ...)

            # Wait for driver pod to be ready
            if client.wait_for_pod_ready(response.submission_id, timeout=120):
                # Get logs
                for line in client.get_logs(response.submission_id):
                    print(line)
            ```

        Note:
            This method is only supported by OperatorBackend.
            For GatewayBackend, it always returns True immediately.
        """
        # Only OperatorBackend supports this
        if hasattr(self.backend, "wait_for_pod_ready"):
            return self.backend.wait_for_pod_ready(submission_id, executor_id, timeout)
        # For other backends, just return True
        return True

    def close(self):
        """Close backend connections and clean up resources.

        Should be called when done using the client to properly clean up.

        Example:
            ```python
            client = SparkClient()
            try:
                # Use client...
                pass
            finally:
                client.close()
            ```
        """
        self.backend.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures cleanup."""
        self.close()

    # =========================================================================
    # Session-Oriented Methods (for Spark Connect)
    # =========================================================================

    def create_session(
        self,
        app_name: str,
        **kwargs: Any,
    ) -> ManagedSparkSession:
        """Create a new Spark Connect session (ConnectBackend only).

        This method creates an interactive Spark session for exploratory data
        analysis, iterative development, and notebook-style workflows.

        Note: Only available with ConnectBackend. OperatorBackend and GatewayBackend
        are batch-oriented and will raise NotImplementedError.

        Args:
            app_name: Name for the session/application
            **kwargs: Additional Spark configuration options

        Returns:
            ManagedSparkSession instance

        Raises:
            NotImplementedError: If backend doesn't support sessions
            RuntimeError: If session creation fails

        Example:
            ```python
            from kubeflow.spark import SparkClient, ConnectBackendConfig

            # Initialize with ConnectBackend
            config = ConnectBackendConfig(connect_url="sc://spark-cluster:15002")
            client = SparkClient(backend_config=config)

            # Create session
            session = client.create_session(app_name="data-analysis")

            # Use PySpark API
            df = session.sql("SELECT * FROM table")
            result = df.collect()

            # Cleanup
            session.close()
            ```
        """
        return self.backend.create_session(app_name=app_name, **kwargs)

    def get_session_status(self, session_id: str) -> SessionInfo:
        """Get status of a Spark Connect session (ConnectBackend only).

        Args:
            session_id: Session UUID

        Returns:
            SessionInfo with session metadata and metrics

        Raises:
            NotImplementedError: If backend doesn't support sessions
            ValueError: If session not found

        Example:
            ```python
            session = client.create_session("my-app")
            info = client.get_session_status(session.session_id)
            print(f"Session state: {info.state}")
            print(f"Queries executed: {info.metrics.queries_executed}")
            ```
        """
        return self.backend.get_session_status(session_id)

    def list_sessions(self) -> List[SessionInfo]:
        """List all active Spark Connect sessions (ConnectBackend only).

        Returns:
            List of SessionInfo objects

        Raises:
            NotImplementedError: If backend doesn't support sessions

        Example:
            ```python
            sessions = client.list_sessions()
            for session_info in sessions:
                print(f"{session_info.app_name}: {session_info.state}")
            ```
        """
        return self.backend.list_sessions()

    def close_session(self, session_id: str, release: bool = True) -> Dict[str, Any]:
        """Close a Spark Connect session (ConnectBackend only).

        Args:
            session_id: Session UUID to close
            release: If True, release session resources on server

        Returns:
            Dictionary with closure response

        Raises:
            NotImplementedError: If backend doesn't support sessions
            ValueError: If session not found

        Example:
            ```python
            session = client.create_session("my-app")
            # ... use session ...
            client.close_session(session.session_id)
            ```
        """
        return self.backend.close_session(session_id, release=release)


# Convenience factory methods for backward compatibility
def create_operator_client(
    namespace: Optional[str] = None,
    **kwargs,
) -> SparkClient:
    """Create a SparkClient with Operator backend.

    Args:
        namespace: Kubernetes namespace
        **kwargs: Additional OperatorBackendConfig parameters

    Returns:
        SparkClient instance with OperatorBackend

    Example:
        ```python
        client = create_operator_client(namespace="spark-jobs")
        ```
    """
    config = OperatorBackendConfig(namespace=namespace, **kwargs)
    return SparkClient(backend_config=config)


def create_gateway_client(
    gateway_url: str,
    user: Optional[str] = None,
    **kwargs,
) -> SparkClient:
    """Create a SparkClient with Gateway backend.

    Args:
        gateway_url: URL of the Batch Processing Gateway
        user: Optional username for authentication
        **kwargs: Additional GatewayBackendConfig parameters

    Returns:
        SparkClient instance with GatewayBackend

    Example:
        ```python
        client = create_gateway_client(gateway_url="http://gateway:8080", user="myuser")
        ```
    """
    config = GatewayBackendConfig(gateway_url=gateway_url, user=user, **kwargs)
    return SparkClient(backend_config=config)
