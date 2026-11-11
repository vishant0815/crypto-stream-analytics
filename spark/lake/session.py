"""A SparkSession with Hive support: table definitions in PostgreSQL, data on HDFS."""

import os
import time

from pyspark.sql import SparkSession

from lake.config import settings


def create_spark(app_name: str) -> SparkSession:
    os.environ["TZ"] = "UTC"
    time.tzset()

    ms = settings.metastore_jdbc
    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.hadoop.fs.defaultFS", settings.hdfs_root)
        .config("spark.sql.warehouse.dir", settings.warehouse_path)
        # Hive metastore in PostgreSQL. The schema is created by
        # db/migrations/005_hive_metastore_schema.sql, never on the fly.
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", ms["url"])
        .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", ms["driver"])
        .config("spark.hadoop.javax.jdo.option.ConnectionUserName", ms["user"])
        .config("spark.hadoop.javax.jdo.option.ConnectionPassword", ms["password"])
        .config("spark.hadoop.datanucleus.schema.autoCreateAll", "false")
        .config("spark.hadoop.hive.metastore.schema.verification", "false")
        # INSERT OVERWRITE replaces only the dt partitions being written
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.hive.exec.dynamic.partition", "true")
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
    )
    # Local runs: connector jars. The PostgreSQL driver must be on the JVM's
    # startup classpath, because the metastore finds it through DriverManager.
    # (The Docker image has all of them in /opt/spark/jars already.)
    extra = os.getenv("SPARK_EXTRA_JARS")
    if extra:
        builder = builder.config("spark.jars", extra)
        driver = [j for j in extra.split(",") if "postgresql" in j]
        if driver:
            builder = builder.config("spark.driver.extraClassPath", driver[0])

    spark = builder.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel(settings.log_level)
    return spark


def hadoop_fs(spark: SparkSession, path: str):
    """The Hadoop FileSystem for a path (hdfs://, file://, ...), and a Path factory."""
    jvm = spark._jvm
    Path = jvm.org.apache.hadoop.fs.Path
    fs = Path(path).getFileSystem(spark._jsc.hadoopConfiguration())
    return fs, Path
