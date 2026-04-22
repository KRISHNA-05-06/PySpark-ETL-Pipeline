from pyspark.sql import SparkSession
import sys
import os

python_path = sys.executable
java_home = r"C:\Program Files\Java\jdk-21.0.10"

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path
os.environ["JAVA_HOME"] = java_home
os.environ["PATH"] = java_home + r"\bin;" + os.environ["PATH"]

def main():
    spark = None
    try:
        print("Using Python:", sys.executable)
        print("Using JAVA_HOME:", os.environ["JAVA_HOME"])

        spark = (
            SparkSession.builder
            .appName("OnlineOrders")
            .config("spark.pyspark.python", python_path)
            .config("spark.pyspark.driver.python", python_path)
            .getOrCreate()
        )

        df = spark.read.csv("data/raw/online_orders.csv", header=True)

        df.show(5, truncate=False)
        df.printSchema()
        df.describe().show()

        print("Rows with $ in price:", df.filter(df.price.contains("$")).count())
        print("Rows with TEST in customer_id:", df.filter(df.customer_id.contains("TEST")).count())
        print("Rows that start with CUST in customer_id:", df.filter(df.customer_id.contains("CUST")).count())
        print("Rows with Test in region:", df.filter(df.region.contains("Test")).count())

    finally:
        if spark is not None:
            spark.stop()
            print("Spark session stopped.")

if __name__ == "__main__":
    main()