"""Example demonstrating per-job image specification for Spark applications.

This example shows how to override the default Spark image for specific jobs,
enabling use cases like:
- Testing new Spark versions without cluster changes
- Using different dependency sets per job
- Optimizing costs with minimal images for simple jobs
- Running GPU-enabled jobs with ML libraries
"""

from kubeflow.spark import BatchSparkClient, OperatorBackendConfig

# Initialize client with default image
config = OperatorBackendConfig(
    namespace="spark-jobs",
    default_spark_image="gcr.io/spark-operator/spark-py",  # Default for all jobs
)
client = BatchSparkClient(backend_config=config)

print("Example 1: Using default image (no override)")
print("=" * 60)

# Uses default: gcr.io/spark-operator/spark-py:3.5.0
response1 = client.submit_application(
    app_name="default-image-job",
    main_application_file="local:///opt/spark/examples/src/main/python/pi.py",
    driver_cores=1,
    driver_memory="512m",
    executor_cores=1,
    executor_memory="512m",
    num_executors=2,
)
print(f"Submitted job: {response1.submission_id}")
print(f"Using default image: gcr.io/spark-operator/spark-py:3.5.0")
print()

print("Example 2: Test new Spark version")
print("=" * 60)

# Override with newer Spark version
response2 = client.submit_application(
    app_name="test-spark-351",
    image="gcr.io/spark-operator/spark-py:3.5.1",  # Override default
    main_application_file="local:///opt/spark/examples/src/main/python/pi.py",
    driver_cores=1,
    driver_memory="512m",
    executor_cores=1,
    executor_memory="512m",
    num_executors=2,
)
print(f"Submitted job: {response2.submission_id}")
print(f"Using custom image: gcr.io/spark-operator/spark-py:3.5.1")
print()

print("Example 3: Different images for different workloads")
print("=" * 60)

# Heavy ML job with GPU-enabled image
ml_response = client.submit_application(
    app_name="ml-feature-engineering",
    image="company-registry.io/spark-ml-gpu:3.5.0",  # Custom ML image
    main_application_file="s3a://my-bucket/ml/feature_extraction.py",
    image_pull_policy="Always",  # Always pull latest
    driver_cores=4,
    driver_memory="16g",
    executor_cores=4,
    executor_memory="32g",
    num_executors=10,
    spark_conf={
        "spark.executor.resource.gpu.amount": "1",
        "spark.task.resource.gpu.amount": "1",
    },
)
print(f"ML job submitted: {ml_response.submission_id}")
print("Using ML image: company-registry.io/spark-ml-gpu:3.5.0")
print()

# Lightweight data aggregation with minimal image
agg_response = client.submit_application(
    app_name="daily-aggregation",
    image="company-registry.io/spark-minimal:3.5.0",  # Minimal 200MB image
    main_application_file="s3a://my-bucket/etl/aggregate.py",
    driver_cores=1,
    driver_memory="1g",
    executor_cores=2,
    executor_memory="4g",
    num_executors=5,
)
print(f"Aggregation job submitted: {agg_response.submission_id}")
print("Using minimal image: company-registry.io/spark-minimal:3.5.0")
print("Cost savings: 200MB image vs 2GB ML image")
print()

print("Example 4: Different Python library versions")
print("=" * 60)

# Job requiring pandas 2.0
pandas_job = client.submit_application(
    app_name="pandas-transform",
    image="company-registry.io/spark-pandas-2.0:3.5.0",
    main_application_file="s3a://my-bucket/etl/pandas_transform.py",
    driver_cores=2,
    driver_memory="4g",
    executor_cores=2,
    executor_memory="8g",
    num_executors=8,
)
print(f"Pandas job submitted: {pandas_job.submission_id}")
print("Using pandas 2.0 image")
print()

# Job requiring scikit-learn 1.3
sklearn_job = client.submit_application(
    app_name="sklearn-pipeline",
    image="company-registry.io/spark-sklearn-1.3:3.5.0",
    main_application_file="s3a://my-bucket/ml/sklearn_pipeline.py",
    driver_cores=2,
    driver_memory="4g",
    executor_cores=2,
    executor_memory="8g",
    num_executors=8,
)
print(f"Sklearn job submitted: {sklearn_job.submission_id}")
print("Using sklearn 1.3 image")
print()

print("Image Precedence Rules:")
print("=" * 60)
print("1. Per-job 'image' parameter (highest priority)")
print("2. Backend config 'default_spark_image'")
print("3. Built-in default: gcr.io/spark-operator/spark-py:{version}")
print()

print("Benefits of per-job images:")
print("- Test new versions without cluster reconfiguration")
print("- Isolate dependencies between jobs")
print("- Optimize costs with right-sized images")
print("- Support multiple teams with different requirements")
