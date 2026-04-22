from pyspark.sql import SparkSession
import logging
import sys
import traceback
from datetime import datetime
import os

# Import our ETL functions
from src.etl_pipeline import *

def setup_logging():
    """Basic logging setup"""

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'logs/etl_run_{datetime.now().strftime("%Y%m%d")}.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

    return logging.getLogger(__name__)

python_path = sys.executable
java_home = r"C:\Program Files\Java\jdk-21.0.10"

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path
os.environ["JAVA_HOME"] = java_home
os.environ["PATH"] = java_home + r"\bin;" + os.environ["PATH"]

def create_spark_session():
    """Create and return a Spark session"""
    return SparkSession.builder \
        .appName("Grocery ETL Pipeline") \
        .getOrCreate()

def main():
    """Main ETL pipeline"""

    # Create necessary dierectories
    os.makedirs('logs', exist_ok=True)
    os.makedirs('data/processed/orders', exist_ok=True)

    logger = setup_logging()
    logger.info("Starting Grocery ETL Pipeline")

    # Track runtime
    start_time = datetime.now()

    spark = None
    try:
        # Initialize Spark
        spark = create_spark_session()
        logger.info("Spark session created")

        # Extract
        raw_df = extract_all_data(spark)
        logger.info(f"Extracted {raw_df.count()} raw records")

        # Transform
        clean_df = transform_orders(raw_df)
        logger.info(f"Transformed to {clean_df.count()} clean records")

        # Load
        output_path = "data/processed/orders"
        load_to_csv(spark, clean_df, output_path)

        # Sanity Check
        sanity_check_data(spark, output_path)

        # Create summary
        summary = create_summary_report(clean_df)

        # Calculate runtime
        runtime = (datetime.now() - start_time).total_seconds()
        logger.info(f"Pipeline completed successfully in {runtime:.2f} seconds")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        logger.error(traceback.format_exc())
        raise


    finally:
        if spark is not None:
            spark.stop()
            print("Spark session stopped.")

if __name__ == "__main__":
    main()