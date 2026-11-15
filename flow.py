import subprocess

from prefect import flow, task, get_run_logger

import config


@task(name="Run Spark job", log_prints=True)
def run_spark_job():
    logger = get_run_logger()

    command = [
        config.SPARK_SUBMIT,
        "--master", "yarn",
        "--deploy-mode", "client",
        "--driver-memory", "1g",
        "--executor-memory", "1g",
        "--conf", f"spark.driver.host={config.DRIVER_HOST}",
        "--conf", f"spark.driver.bindAddress={config.DRIVER_HOST}",
        "--conf", "spark.hadoop.hive.metastore.uris=thrift://localhost:9083",
        "--conf", "spark.sql.catalogImplementation=hive",
        "--conf", "spark.yarn.submit.waitAppCompletion=true",
        str(config.PROJECT_DIR / "spark_job.py"),
    ]

    logger.info("Starting Spark application on YARN")
    logger.info("Command: %s", " ".join(command))

    subprocess.run(
        command,
        cwd=str(config.PROJECT_DIR),
        env=config.ENVIRONMENT,
        check=True,
    )

    logger.info("Spark application completed successfully")


@task(name="Verify HDFS output")
def verify_hdfs_output():
    logger = get_run_logger()

    logger.info("Checking output directory in HDFS")

    subprocess.run(
        ["hdfs", "dfs", "-test", "-e", config.OUTPUT_PATH],
        env=config.ENVIRONMENT,
        check=True,
    )

    subprocess.run(
        ["hdfs", "dfs", "-ls", "-R", config.OUTPUT_PATH],
        env=config.ENVIRONMENT,
        check=True,
    )

    logger.info("HDFS output successfully verified")


@flow(name="Team 12 - Homework 5 Data Pipeline", log_prints=True)
def homework5_pipeline():
    run_spark_job()
    verify_hdfs_output()


if __name__ == "__main__":
    homework5_pipeline()
