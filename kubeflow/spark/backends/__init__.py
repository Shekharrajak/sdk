"""Spark backends for different execution environments."""

from kubeflow.spark.backends.base import SparkBackend
from kubeflow.spark.backends.gateway import GatewayBackend, GatewayBackendConfig
from kubeflow.spark.backends.operator import OperatorBackend, OperatorBackendConfig

__all__ = [
    "SparkBackend",
    "OperatorBackend",
    "OperatorBackendConfig",
    "GatewayBackend",
    "GatewayBackendConfig",
]
