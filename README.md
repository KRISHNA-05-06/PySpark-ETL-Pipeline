# 🛒 Grocery ETL Pipeline using PySpark

## 📌 Overview
This project demonstrates a complete **ETL (Extract, Transform, Load) pipeline** built using **PySpark**.  
The pipeline processes messy real-world sales data from multiple sources and converts it into clean, structured data for analytics.

---

## 🎯 Problem Statement
The raw data had several inconsistencies:
- Mixed date formats (yyyy-MM-dd, MM/dd/yyyy, dd-MM-yyyy)
- Prices with symbols (e.g., "$12.99", "12.99")
- Inconsistent customer IDs (numeric vs "CUST_123")
- Duplicate records
- Test data mixed with production data
- Missing/null values

---

## 🏗️ Project Structure

```bash
grocery_etl/
│
├── data/
│   ├── raw/           # Raw input CSV files
│   └── processed/     # Cleaned output data
│
├── src/
│   └── etl_pipeline.py   # ETL logic
│
├── main.py              # Pipeline orchestration
├── requirements.txt
└── README.md

⚙️ Technologies Used
Python
PySpark (Spark 4.0)
Pandas (for local CSV output)
Logging module

🔄 ETL Pipeline Workflow

1. Extract
Reads CSV files from multiple sources:
Online orders
Store orders
Mobile orders
Uses explicit schema to prevent data loss
Handles malformed data using PERMISSIVE mode
Combines datasets using unionByName

2. Transform
Data cleaning and standardization:
✅ Standardized customer IDs → CUST_123
💲 Cleaned price values (removed symbols, converted to numeric)
📅 Parsed multiple date formats into a standard format
🧹 Removed test and invalid records
🔁 Eliminated duplicate orders
📊 Added derived columns:
total_amount
processing_date
year, month

3. Load
Writes cleaned data to CSV (orders.csv)
Uses Pandas for local development
Production-ready alternative: Spark .write() APIs
📊 Data Validation

Performed sanity checks using Spark SQL:

Total record count verification
Zero-price detection
Date range validation

📈 Summary Report

Generates key metrics:

Total orders processed
Unique customers
Unique products
Total revenue
Date range
Regional distribution

🚀 How to Run
1. Install dependencies
pip install -r requirements.txt
2. Run the pipeline
spark-submit main.py
3. (Optional) Increase memory
spark-submit --driver-memory 4g main.py

⚠️ Key Learnings
Built a complete ETL pipeline from scratch
Handled messy real-world data (not just clean datasets)
Used defensive data engineering techniques
Implemented logging and error handling
Applied modular ETL design (Extract → Transform → Load)
Used Spark DataFrame transformations effectively
Applied Spark SQL for validation
Understood limitations of .toPandas() for large datasets

🐞 Common Issues & Fixes
Memory Error (OutOfMemoryError)
spark-submit --driver-memory 4g main.py

Or switch to Spark native writer:

df.write.mode("overwrite").option("header", "true").csv(output_path)
Schema Issues
df.printSchema()
print(df.columns)
Slow Performance
df.cache()
df.unpersist()

🔮 Future Improvements
Replace CSV output with Parquet format
Add Airflow scheduling
Implement automated data quality tests
Optimize performance (partitioning, caching)
Deploy on AWS / Databricks

📌 Example Output

The cleaned dataset includes:
Standardized customer IDs
Parsed dates
Clean numeric prices
Calculated revenue fields

👨‍💻 Author

Sri Krishna Sai Kota
MS Computer Science @ University of South Florida
Aspiring Data Engineer