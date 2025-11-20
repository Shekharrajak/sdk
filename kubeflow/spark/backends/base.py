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

"""Base backend interface for Spark applications."""

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from kubeflow.spark.models import ApplicationStatus, SessionInfo, SparkApplicationResponse

if TYPE_CHECKING:
    from kubeflow.spark.session import ManagedSparkSession


class SparkBackend(abc.ABC):
    """Base class for Spark backends.

    This abstract base class defines the interface that all Spark backends
    must implement. Different backends can execute Spark applications in
    different environments (Kubernetes with Spark Operator, Gateway, local, etc).
    """

    @abc.abstractmethod
    def submit_application(
        self,
        app_name: str,
        main_application_file: str,
        spark_version: str,
        app_type: str,
        driver_cores: int,
        driver_memory: str,
        executor_cores: int,
        executor_memory: str,
        num_executors: int,
        queue: Optional[str],
        arguments: Optional[List[str]],
        python_version: str,
        spark_conf: Optional[Dict[str, str]],
        hadoop_conf: Optional[Dict[str, str]],
        env_vars: Optional[Dict[str, str]],
        deps: Optional[Dict[str, List[str]]],
        **kwargs: Any,
    ) -> SparkApplicationResponse:
        """Submit a Spark application.

        Args:
            app_name: Name of the application
            main_application_file: Path to main application file
            spark_version: Spark version to use
            app_type: Application type (Python, Scala, Java, R)
            driver_cores: Number of cores for driver
            driver_memory: Memory for driver (e.g., "4g")
            executor_cores: Number of cores per executor
            executor_memory: Memory per executor (e.g., "8g")
            num_executors: Number of executors
            queue: Queue/namespace to submit to
            arguments: Application arguments
            python_version: Python version for PySpark apps
            spark_conf: Spark configuration properties
            hadoop_conf: Hadoop configuration properties
            env_vars: Environment variables
            deps: Dependencies (jars, py files, files)
            **kwargs: Additional backend-specific parameters

        Returns:
            SparkApplicationResponse with submission details

        Raises:
            RuntimeError: If submission fails
            TimeoutError: If submission times out
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_status(self, submission_id: str) -> ApplicationStatus:
        """Get status of a Spark application.

        Args:
            submission_id: Submission ID returned from submit_application

        Returns:
            ApplicationStatus with current status

        Raises:
            RuntimeError: If request fails
            TimeoutError: If request times out
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def delete_application(self, submission_id: str) -> Dict[str, Any]:
        """Delete a Spark application.

        Args:
            submission_id: Submission ID to delete

        Returns:
            Dictionary with deletion response

        Raises:
            RuntimeError: If deletion fails
            TimeoutError: If deletion times out
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_logs(
        self,
        submission_id: str,
        executor_id: Optional[str] = None,
        follow: bool = False,
    ) -> Iterator[str]:
        """Get application logs.

        Args:
            submission_id: Submission ID
            executor_id: Optional executor ID (if not provided, returns driver logs)
            follow: Whether to stream logs in real-time

        Yields:
            Log lines as strings

        Raises:
            RuntimeError: If request fails
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def list_applications(
        self,
        namespace: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
    ) -> List[ApplicationStatus]:
        """List Spark applications.

        Args:
            namespace: Optional namespace filter
            labels: Optional label filters

        Returns:
            List of ApplicationStatus objects

        Raises:
            RuntimeError: If request fails
            TimeoutError: If request times out
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def wait_for_completion(
        self,
        submission_id: str,
        timeout: int = 3600,
        polling_interval: int = 10,
    ) -> ApplicationStatus:
        """Wait for Spark application to complete.

        Args:
            submission_id: Submission ID to monitor
            timeout: Maximum time to wait in seconds
            polling_interval: Polling interval in seconds

        Returns:
            Final ApplicationStatus

        Raises:
            TimeoutError: If application doesn't complete within timeout
            RuntimeError: If monitoring fails
        """
        raise NotImplementedError()

    def close(self):
        """Close any open connections or resources.

        Subclasses can override this to clean up resources.
        """
        pass

    # =========================================================================
    # Session-Oriented Methods (for Spark Connect backends)
    # =========================================================================
    # These methods are optional and only need to be implemented by backends
    # that support interactive session management (e.g., ConnectBackend).
    # Batch-oriented backends (OperatorBackend, GatewayBackend) can leave
    # these as default implementations that raise NotImplementedError.

    def create_session(
        self,
        app_name: str,
        **kwargs: Any,
    ) -> "ManagedSparkSession":
        """Create a new Spark Connect session.

        This method is for backends that support interactive, session-based
        workloads (e.g., Spark Connect). Batch-oriented backends should raise
        NotImplementedError.

        Args:
            app_name: Name for the session/application
            **kwargs: Backend-specific configuration

        Returns:
            ManagedSparkSession instance

        Raises:
            NotImplementedError: If backend doesn't support sessions
            RuntimeError: If session creation fails
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support session-based operations. "
            "Use ConnectBackend for interactive Spark sessions."
        )

    def get_session_status(self, session_id: str) -> SessionInfo:
        """Get status of a Spark Connect session.

        Args:
            session_id: Session UUID

        Returns:
            SessionInfo with session metadata and metrics

        Raises:
            NotImplementedError: If backend doesn't support sessions
            RuntimeError: If request fails
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support session-based operations."
        )

    def list_sessions(self) -> List[SessionInfo]:
        """List all active Spark Connect sessions.

        Returns:
            List of SessionInfo objects

        Raises:
            NotImplementedError: If backend doesn't support sessions
            RuntimeError: If request fails
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support session-based operations."
        )

    def close_session(self, session_id: str, release: bool = True) -> Dict[str, Any]:
        """Close a Spark Connect session.

        Args:
            session_id: Session UUID to close
            release: If True, release session resources on server

        Returns:
            Dictionary with closure response

        Raises:
            NotImplementedError: If backend doesn't support sessions
            RuntimeError: If closure fails
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support session-based operations."
        )
