from pathlib import Path
import os

PROJECT_DIR = Path(__file__).resolve().parent

SPARK_HOME = "/opt/spark"
SPARK_SUBMIT = f"{SPARK_HOME}/bin/spark-submit"

HADOOP_CONF_DIR = "/opt/hadoop/etc/hadoop"

INPUT_PATH = "hdfs:///user/team/hw4_input/customers.csv"
OUTPUT_PATH = "hdfs:///user/team/hw5_output/customer_stats"

DATABASE_NAME = "team12_hw5"
TABLE_NAME = "customer_stats"
FULL_TABLE_NAME = f"{DATABASE_NAME}.{TABLE_NAME}"

WAREHOUSE_PATH = "hdfs:///user/team/hw5_warehouse"

DRIVER_HOST = "10.12.0.10"

ENVIRONMENT = os.environ.copy()
ENVIRONMENT["SPARK_HOME"] = SPARK_HOME
ENVIRONMENT["HADOOP_CONF_DIR"] = HADOOP_CONF_DIR
ENVIRONMENT["YARN_CONF_DIR"] = HADOOP_CONF_DIR
