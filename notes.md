Apache Spark: Open-source distributed computing engine developed by the Apache Software Foundation. It is designed to process large datasets quickly and efficiently across a cluster of machines. 
    It's key features include:
    1. High Performance: Much faster than Hadoop, thanks to in-memory computing.
    2. Multi-language Support: Works with Python, Scala, Java and R.
    3. All-in-One Engine: Handles batch, streaming, ML and graph processing.
    4. Easy to Use: Offers simple, high-level APIs built on the MapReduce model.

Pyspark:PySpark is the Python API for Apache Spark, allowing Python developers to use the full power of Spark’s distributed computing framework with familiar Python syntax. It bridges the gap between Python’s ease of use and Spark’s processing power. 
    It's key features include:
    1. Python-Friendly: Build Spark applications using pure Python great for data scientists and engineers.
    2. Handles Big Data: Efficiently process huge datasets across multiple machines.
    3. Rich Libraries: Includes modules for SQL (pyspark.sql), machine learning (pyspark.ml), and streaming (pyspark.streaming).
    4.DataFrame & SQL API: Work with structured data using powerful, SQL-like operations.

PySpark Modules:
Module | Description
pyspark.sql: | Work with structured data using DataFrames and SQL queries.
pyspark.ml: | Build machine learning pipelines (classification, regression, clustering, etc.).
pyspark.streaming: | Process real-time data streams (e.g., Twitter feed, logs).
pyspark.graphx: | Handle graph computations and social network analysis (Scala/Java primarily).

PySpark Works:

Driver Program: Your Python script that initiates and controls the Spark job.
SparkContext: Connects the driver to the Spark cluster and manages job configuration.
RDDs/DataFrames: Data structures that are distributed and processed in parallel.
Cluster Manager: Schedules and allocates resources to worker nodes (e.g., YARN, Mesos, Kubernetes).
Executor Nodes: Run the actual tasks in parallel and return results to the driver.



