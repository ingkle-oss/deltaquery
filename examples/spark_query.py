import os
import argparse
import logging
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--s3endpoint",
        help="s3 endpoint",
        default="http://127.0.0.1:9000",
    )
    parser.add_argument("--s3accesskey", help="s3 accesskey", required=True)
    parser.add_argument("--s3secretkey", help="s3 secretkey", required=True)
    parser.add_argument(
        "-i",
        "--input",
        help="input path",
        default="s3a://delta",
    )
    parser.add_argument(
        "-q",
        "--query",
        help="sql query",
        default="select * from test0",
    )
    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logger = logging.getLogger("SparkQuery")
    logger.setLevel(level=logging.INFO)
    logger_handler = logging.StreamHandler()
    logger_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )
    logger.addHandler(logger_handler)

    so = SparkConf()
    so.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    so.set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    so.set("spark.hadoop.fs.s3a.endpoint", args.s3endpoint)
    so.set("spark.hadoop.fs.s3a.access.key", args.s3accesskey)
    so.set("spark.hadoop.fs.s3a.secret.key", args.s3secretkey)
    so.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    so.set("spark.hadoop.fs.s3a.path.style.access", "true")
    so.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")

    ss = SparkSession.builder.config(conf=so).appName("spark-query").getOrCreate()
    ss.sparkContext.setLogLevel(args.loglevel)

    df = ss.read.format("delta").load(args.input)
    df.createOrReplaceTempView(os.path.basename(args.input))

    ss.sql(args.query).show()
