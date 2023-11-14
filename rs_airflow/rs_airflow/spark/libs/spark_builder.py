import multiprocessing
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession

from rs_airflow.spark.libs.driver_managers import DriverManager


class SparkBuilder:
    """
    Airflow's related details for constructing a SparkSession

    Returns:
        SparkSession: To working with **Local Spark**
    """

    @classmethod
    def generate_spark_session(cls, conf: SparkConf | None = None) -> SparkSession:
        spark_conf = SparkConf()

        joined_driver_path = DriverManager.get_drivers_path_list()
        spark_conf.set("spark.jars", joined_driver_path)

        spark_executor_cores = max(multiprocessing.cpu_count() - 2, 1)
        spark_conf.set("spark.default.parallelism", str(spark_executor_cores))

        machine_total_memory_MiB = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / (1024.0**2)
        driver_provided_memory_MiB = max(machine_total_memory_MiB - 512, 512)
        spark_conf.set("spark.driver.memory", f"{driver_provided_memory_MiB:.0f}m")

        if conf:
            spark_conf.setAll(conf.getAll())

        return SparkSession.builder.config(conf=spark_conf).master("local[*]").getOrCreate()  # type: ignore
