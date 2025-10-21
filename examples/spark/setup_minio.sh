#!/bin/bash
set -e

echo "================================================================================"
echo "Setting up MinIO (S3-compatible storage) for Spark Examples"
echo "================================================================================"
echo ""

# Configuration
NAMESPACE="default"
MINIO_ROOT_USER="minioadmin"
MINIO_ROOT_PASSWORD="minioadmin"
MINIO_SERVICE="minio-service"
MINIO_ENDPOINT="minio-service.default.svc.cluster.local:9000"

echo "Step 1: Deploying MinIO to Kubernetes..."
echo "--------------------------------------------------------------------------------"

# Create MinIO deployment
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: minio-pvc
  namespace: ${NAMESPACE}
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: ${NAMESPACE}
spec:
  selector:
    matchLabels:
      app: minio
  replicas: 1
  template:
    metadata:
      labels:
        app: minio
    spec:
      containers:
      - name: minio
        image: quay.io/minio/minio:latest
        command:
        - /bin/bash
        - -c
        args:
        - minio server /data --console-address :9001
        env:
        - name: MINIO_ROOT_USER
          value: "${MINIO_ROOT_USER}"
        - name: MINIO_ROOT_PASSWORD
          value: "${MINIO_ROOT_PASSWORD}"
        ports:
        - containerPort: 9000
          name: api
        - containerPort: 9001
          name: console
        volumeMounts:
        - name: storage
          mountPath: /data
      volumes:
      - name: storage
        persistentVolumeClaim:
          claimName: minio-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: ${MINIO_SERVICE}
  namespace: ${NAMESPACE}
spec:
  type: ClusterIP
  ports:
    - port: 9000
      targetPort: 9000
      protocol: TCP
      name: api
    - port: 9001
      targetPort: 9001
      protocol: TCP
      name: console
  selector:
    app: minio
EOF

echo "  ✓ MinIO deployed"
echo ""

echo "Step 2: Waiting for MinIO to be ready..."
echo "--------------------------------------------------------------------------------"
kubectl wait --for=condition=ready pod -l app=minio -n ${NAMESPACE} --timeout=120s
echo "  ✓ MinIO is ready"
echo ""

echo "Step 3: Port-forward MinIO console (for browser access)..."
echo "--------------------------------------------------------------------------------"
echo "  To access MinIO console, run in another terminal:"
echo "    kubectl port-forward svc/${MINIO_SERVICE} 9001:9001 -n ${NAMESPACE}"
echo "  Then open: http://localhost:9001"
echo "  Login: ${MINIO_ROOT_USER} / ${MINIO_ROOT_PASSWORD}"
echo ""

echo "Step 4: Installing MinIO client (mc) in a pod..."
echo "--------------------------------------------------------------------------------"

# Create a pod with mc client for bucket setup
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: minio-client
  namespace: ${NAMESPACE}
spec:
  containers:
  - name: mc
    image: quay.io/minio/mc:latest
    command: ["/bin/sh"]
    args: ["-c", "sleep 3600"]
  restartPolicy: Never
EOF

kubectl wait --for=condition=ready pod/minio-client -n ${NAMESPACE} --timeout=60s
echo "  ✓ MinIO client pod ready"
echo ""

echo "Step 5: Configuring MinIO and creating buckets..."
echo "--------------------------------------------------------------------------------"

# Configure mc to point to our MinIO
kubectl exec -n ${NAMESPACE} minio-client -- mc alias set myminio http://${MINIO_SERVICE}:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}

# Create buckets for examples
kubectl exec -n ${NAMESPACE} minio-client -- mc mb myminio/spark-scripts --ignore-existing
kubectl exec -n ${NAMESPACE} minio-client -- mc mb myminio/spark-data --ignore-existing
kubectl exec -n ${NAMESPACE} minio-client -- mc mb myminio/spark-output --ignore-existing

echo "  ✓ Created buckets:"
echo "    - spark-scripts (for PySpark scripts)"
echo "    - spark-data (for input data)"
echo "    - spark-output (for results)"
echo ""

# List buckets to verify
echo "Verifying buckets:"
kubectl exec -n ${NAMESPACE} minio-client -- mc ls myminio/
echo ""

echo "Step 6: Creating sample PySpark scripts..."
echo "--------------------------------------------------------------------------------"

# Create batch job script
cat > /tmp/batch_job.py <<'BATCH_SCRIPT'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, current_timestamp
from pyspark.sql.types import *
from datetime import datetime

spark = SparkSession.builder.appName("Batch Job").getOrCreate()

print("\n" + "="*80)
print("SCHEDULED BATCH JOB - DAILY PROCESSING")
print("="*80)

# Configuration
BATCH_DATE = datetime.now().strftime("%Y-%m-%d")
JOB_ID = f"batch_{BATCH_DATE.replace('-', '')}"

print(f"\n[CONFIG] Batch Configuration:")
print(f"  • Batch Date: {BATCH_DATE}")
print(f"  • Job ID: {JOB_ID}")

# Create sample transaction data
schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("date", StringType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("amount", DoubleType(), False),
])

transactions_data = [
    (1, BATCH_DATE, 101, 150.00),
    (2, BATCH_DATE, 102, 250.00),
    (3, BATCH_DATE, 103, 75.00),
    (4, BATCH_DATE, 101, 300.00),
    (5, BATCH_DATE, 104, 500.00),
]

df = spark.createDataFrame(transactions_data, schema)

print(f"\n[EXTRACT] Loaded {df.count()} transactions")
print("\nSample transactions:")
df.show()

# Transform: Add metadata
df_enriched = df.withColumn("processing_timestamp", current_timestamp()) \
                .withColumn("job_id", col("transaction_id").cast("string"))

print("\n[TRANSFORM] Added metadata columns")

# Aggregate by customer
summary = df_enriched.groupBy("customer_id").agg(
    count("transaction_id").alias("transaction_count"),
    _sum("amount").alias("total_amount")
).orderBy(col("total_amount").desc())

print("\n[LOAD] Customer Summary:")
summary.show()

print(f"\n[COMPLETE] Batch job {JOB_ID} completed successfully!")
print("="*80)
spark.stop()
BATCH_SCRIPT

# Create exploration script
cat > /tmp/exploration.py <<'EXPLORATION_SCRIPT'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, min as _min, max as _max
from pyspark.sql.types import *

spark = SparkSession.builder.appName("DataFrame Exploration").getOrCreate()

print("\n" + "="*80)
print("INTERACTIVE DATAFRAME EXPLORATION")
print("="*80)

# Create sample customer dataset
schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("purchases", IntegerType(), True),
    StructField("total_spent", DoubleType(), True),
])

customers_data = [
    (1, "Alice", 28, "New York", 15, 1250.50),
    (2, "Bob", 35, "Los Angeles", 8, 890.25),
    (3, "Carol", None, "Chicago", 22, 2100.00),  # Missing age
    (4, "David", 42, "Houston", 5, 450.75),
]

df = spark.createDataFrame(customers_data, schema)

print("\nDataset Summary:")
print(f"Total Records: {df.count()}")

print("\nSchema:")
df.printSchema()

print("\nSample Data:")
df.show()

print("\nDescriptive Statistics:")
df.describe().show()

print("\nNull Check:")
df.select([count(col(c).isNull()).alias(c) for c in df.columns]).show()

print("\n" + "="*80)
spark.stop()
EXPLORATION_SCRIPT

# Create CSV analysis script
cat > /tmp/csv_analysis.py <<'CSV_SCRIPT'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count

spark = SparkSession.builder.appName("CSV Analysis").getOrCreate()

print("\n" + "="*80)
print("CSV DATA ANALYSIS")
print("="*80)

# Read sample CSV from MinIO
# For this example, we'll create data in-memory
from pyspark.sql.types import *

schema = StructType([
    StructField("product", StringType()),
    StructField("category", StringType()),
    StructField("quantity", IntegerType()),
    StructField("price", DoubleType()),
])

data = [
    ("Laptop", "Electronics", 2, 1200.00),
    ("Mouse", "Electronics", 5, 25.00),
    ("Keyboard", "Electronics", 3, 75.00),
    ("Desk", "Furniture", 1, 500.00),
    ("Chair", "Furniture", 2, 250.00),
]

df = spark.createDataFrame(data, schema)

print("\nSample Data:")
df.show()

print("\nSales by Category:")
df.groupBy("category").agg(
    count("product").alias("products"),
    _sum("quantity").alias("total_quantity"),
    _sum(col("quantity") * col("price")).alias("revenue")
).show()

print("\n" + "="*80)
spark.stop()
CSV_SCRIPT

# Create ETL script
cat > /tmp/etl_pipeline.py <<'ETL_SCRIPT'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim, current_timestamp

spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()

print("\n" + "="*80)
print("ETL PIPELINE")
print("="*80)

# Extract
print("\n[EXTRACT] Loading data...")
from pyspark.sql.types import *
schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("amount", DoubleType()),
])
data = [(1, " alice ", 100.0), (2, "BOB", 200.0), (3, "carol  ", 150.0)]
df = spark.createDataFrame(data, schema)
print(f"Extracted {df.count()} records")

# Transform
print("\n[TRANSFORM] Cleaning data...")
df_clean = df.withColumn("name", upper(trim(col("name")))) \
             .withColumn("processed_at", current_timestamp())
print("Transformations applied: trim, uppercase, timestamp")

# Load
print("\n[LOAD] Results:")
df_clean.show()

print("\n" + "="*80)
spark.stop()
ETL_SCRIPT

echo "  ✓ Created PySpark scripts"
echo ""

echo "Step 7: Uploading scripts to MinIO..."
echo "--------------------------------------------------------------------------------"

# Upload scripts directly to MinIO using stdin (avoids need for 'tar' in container)
echo "  Uploading batch_job.py..."
kubectl exec -n ${NAMESPACE} minio-client -- sh -c 'cat > /tmp/batch_job.py' < /tmp/batch_job.py
kubectl exec -n ${NAMESPACE} minio-client -- mc cp /tmp/batch_job.py myminio/spark-scripts/

echo "  Uploading exploration.py..."
kubectl exec -n ${NAMESPACE} minio-client -- sh -c 'cat > /tmp/exploration.py' < /tmp/exploration.py
kubectl exec -n ${NAMESPACE} minio-client -- mc cp /tmp/exploration.py myminio/spark-scripts/

echo "  Uploading csv_analysis.py..."
kubectl exec -n ${NAMESPACE} minio-client -- sh -c 'cat > /tmp/csv_analysis.py' < /tmp/csv_analysis.py
kubectl exec -n ${NAMESPACE} minio-client -- mc cp /tmp/csv_analysis.py myminio/spark-scripts/

echo "  Uploading etl_pipeline.py..."
kubectl exec -n ${NAMESPACE} minio-client -- sh -c 'cat > /tmp/etl_pipeline.py' < /tmp/etl_pipeline.py
kubectl exec -n ${NAMESPACE} minio-client -- mc cp /tmp/etl_pipeline.py myminio/spark-scripts/

echo "  Uploading long_running_job.py..."
kubectl exec -n ${NAMESPACE} minio-client -- sh -c 'cat > /tmp/long_running_job.py' < scripts/long_running_job.py
kubectl exec -n ${NAMESPACE} minio-client -- mc cp /tmp/long_running_job.py myminio/spark-scripts/

echo "  ✓ Uploaded scripts to s3://spark-scripts/"
echo ""

# Verify uploads
echo "Verifying uploaded scripts:"
kubectl exec -n ${NAMESPACE} minio-client -- mc ls myminio/spark-scripts/
echo ""

echo "Step 8: Creating Spark S3 access secret..."
echo "--------------------------------------------------------------------------------"

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: spark-s3-credentials
  namespace: ${NAMESPACE}
type: Opaque
stringData:
  access-key: "${MINIO_ROOT_USER}"
  secret-key: "${MINIO_ROOT_PASSWORD}"
  endpoint: "http://${MINIO_ENDPOINT}"
EOF

echo "  ✓ Created secret: spark-s3-credentials"
echo ""

echo "================================================================================"
echo "✅ MinIO Setup Complete!"
echo "================================================================================"
echo ""
echo "MinIO Configuration:"
echo "  • Endpoint: http://${MINIO_ENDPOINT}"
echo "  • Access Key: ${MINIO_ROOT_USER}"
echo "  • Secret Key: ${MINIO_ROOT_PASSWORD}"
echo ""
echo "Buckets Created:"
echo "  • s3://spark-scripts/ - PySpark application scripts"
echo "  • s3://spark-data/ - Input data files"
echo "  • s3://spark-output/ - Output results"
echo ""
echo "Uploaded Scripts:"
echo "  • s3://spark-scripts/batch_job.py"
echo "  • s3://spark-scripts/exploration.py"
echo "  • s3://spark-scripts/csv_analysis.py"
echo "  • s3://spark-scripts/etl_pipeline.py"
echo ""
echo "To access MinIO Console:"
echo "  1. Run: kubectl port-forward svc/${MINIO_SERVICE} 9001:9001 -n ${NAMESPACE}"
echo "  2. Open: http://localhost:9001"
echo "  3. Login: ${MINIO_ROOT_USER} / ${MINIO_ROOT_PASSWORD}"
echo ""
echo "Next steps:"
echo "  → Update examples to use S3 paths"
echo "  → Run: python 03_interactive_dataframe_exploration.py"
echo ""
