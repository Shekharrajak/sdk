"""Tests for OperatorBackend image precedence and configuration."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from kubeflow.spark.backends.operator import OperatorBackend, OperatorBackendConfig
from kubeflow.spark.models import SparkApplicationResponse, ApplicationState


class TestOperatorBackendImagePrecedence:
    """Test image precedence logic in OperatorBackend."""

    @patch("kubeflow.spark.backends.operator.config.load_kube_config")
    @patch("kubeflow.spark.backends.operator.client.ApiClient")
    @patch("kubeflow.spark.backends.operator.client.CustomObjectsApi")
    @patch("kubeflow.spark.backends.operator.client.CoreV1Api")
    def test_per_job_image_overrides_config_default(
        self, mock_core_api, mock_custom_api, mock_api_client, mock_load_config
    ):
        """Test that per-job image parameter overrides backend config default."""
        backend_config = OperatorBackendConfig(
            namespace="test",
            default_spark_image="gcr.io/spark:default"
        )
        backend = OperatorBackend(backend_config)

        mock_custom_api_instance = MagicMock()
        mock_custom_api_instance.create_namespaced_custom_object.return_value = {
            "metadata": {"name": "test-job", "namespace": "test"},
            "status": {"applicationState": {"state": "SUBMITTED"}}
        }
        backend.custom_api = mock_custom_api_instance

        response = backend.submit_application(
            app_name="test-job",
            main_application_file="local:///test.py",
            spark_version="3.5.0",
            app_type="Python",
            image="gcr.io/spark:custom",  # Per-job override
            image_pull_policy="Always",
            driver_cores=1,
            driver_memory="1g",
            executor_cores=1,
            executor_memory="1g",
            num_executors=2,
        )

        call_args = mock_custom_api_instance.create_namespaced_custom_object.call_args
        crd_spec = call_args[1]["body"]["spec"]

        assert crd_spec["image"] == "gcr.io/spark:custom"
        assert crd_spec["imagePullPolicy"] == "Always"

    @patch("kubeflow.spark.backends.operator.config.load_kube_config")
    @patch("kubeflow.spark.backends.operator.client.ApiClient")
    @patch("kubeflow.spark.backends.operator.client.CustomObjectsApi")
    @patch("kubeflow.spark.backends.operator.client.CoreV1Api")
    def test_config_default_when_no_per_job_image(
        self, mock_core_api, mock_custom_api, mock_api_client, mock_load_config
    ):
        """Test that config default is used when no per-job image specified."""
        backend_config = OperatorBackendConfig(
            namespace="test",
            default_spark_image="gcr.io/spark:config-default"
        )
        backend = OperatorBackend(backend_config)

        mock_custom_api_instance = MagicMock()
        mock_custom_api_instance.create_namespaced_custom_object.return_value = {
            "metadata": {"name": "test-job", "namespace": "test"},
            "status": {"applicationState": {"state": "SUBMITTED"}}
        }
        backend.custom_api = mock_custom_api_instance

        response = backend.submit_application(
            app_name="test-job",
            main_application_file="local:///test.py",
            spark_version="3.5.1",
            app_type="Python",
            image=None,  # No per-job image
            image_pull_policy="IfNotPresent",
            driver_cores=1,
            driver_memory="1g",
            executor_cores=1,
            executor_memory="1g",
            num_executors=2,
        )

        call_args = mock_custom_api_instance.create_namespaced_custom_object.call_args
        crd_spec = call_args[1]["body"]["spec"]

        assert crd_spec["image"] == "gcr.io/spark:config-default:3.5.1"
        assert crd_spec["imagePullPolicy"] == "IfNotPresent"

    @patch("kubeflow.spark.backends.operator.config.load_kube_config")
    @patch("kubeflow.spark.backends.operator.client.ApiClient")
    @patch("kubeflow.spark.backends.operator.client.CustomObjectsApi")
    @patch("kubeflow.spark.backends.operator.client.CoreV1Api")
    def test_built_in_default_when_no_config(
        self, mock_core_api, mock_custom_api, mock_api_client, mock_load_config
    ):
        """Test that built-in default is used when no config default."""
        backend_config = OperatorBackendConfig(namespace="test")
        backend = OperatorBackend(backend_config)

        mock_custom_api_instance = MagicMock()
        mock_custom_api_instance.create_namespaced_custom_object.return_value = {
            "metadata": {"name": "test-job", "namespace": "test"},
            "status": {"applicationState": {"state": "SUBMITTED"}}
        }
        backend.custom_api = mock_custom_api_instance

        response = backend.submit_application(
            app_name="test-job",
            main_application_file="local:///test.py",
            spark_version="3.5.0",
            app_type="Python",
            image=None,
            image_pull_policy="IfNotPresent",
            driver_cores=1,
            driver_memory="1g",
            executor_cores=1,
            executor_memory="1g",
            num_executors=2,
        )

        call_args = mock_custom_api_instance.create_namespaced_custom_object.call_args
        crd_spec = call_args[1]["body"]["spec"]

        assert crd_spec["image"].startswith("gcr.io/spark-operator/spark-py:3.5.0")

    @patch("kubeflow.spark.backends.operator.config.load_kube_config")
    @patch("kubeflow.spark.backends.operator.client.ApiClient")
    @patch("kubeflow.spark.backends.operator.client.CustomObjectsApi")
    @patch("kubeflow.spark.backends.operator.client.CoreV1Api")
    def test_image_pull_policy_options(
        self, mock_core_api, mock_custom_api, mock_api_client, mock_load_config
    ):
        """Test different image pull policy options."""
        backend_config = OperatorBackendConfig(namespace="test")
        backend = OperatorBackend(backend_config)

        mock_custom_api_instance = MagicMock()
        mock_custom_api_instance.create_namespaced_custom_object.return_value = {
            "metadata": {"name": "test-job", "namespace": "test"},
            "status": {"applicationState": {"state": "SUBMITTED"}}
        }
        backend.custom_api = mock_custom_api_instance

        for policy in ["Always", "IfNotPresent", "Never"]:
            backend.submit_application(
                app_name=f"test-job-{policy.lower()}",
                main_application_file="local:///test.py",
                spark_version="3.5.0",
                app_type="Python",
                image="gcr.io/spark:test",
                image_pull_policy=policy,
                driver_cores=1,
                driver_memory="1g",
                executor_cores=1,
                executor_memory="1g",
                num_executors=2,
            )

            call_args = mock_custom_api_instance.create_namespaced_custom_object.call_args
            crd_spec = call_args[1]["body"]["spec"]
            assert crd_spec["imagePullPolicy"] == policy
