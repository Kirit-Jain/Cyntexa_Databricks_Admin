from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

CATALOG = spark.conf.get("catalog")
path="/Volumes/dev/bronze/raw"

@dp.table(
    name=f"{CATALOG}.bronze.bronze_customers"
)
def bronze_customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"/Volumes/{CATALOG}/bronze/raw/customers/_schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"/Volumes/{CATALOG}/bronze/raw/customers")
        .select(
            "*",
            col("_metadata.file_name").alias("file_name"),
            col("_metadata.file_path").alias("file_path"),
            current_timestamp().alias("ingestion_date"),
            col("_metadata.file_modification_time").alias("update_date")
        )
    )


@dp.table(
    name=f"{CATALOG}.bronze.bronze_accounts"
)
def bronze_accounts():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"/Volumes/{CATALOG}/bronze/raw/accounts/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"/Volumes/{CATALOG}/bronze/raw/accounts")
        .select(
            "*",
            col("_metadata.file_name").alias("file_name"),
            col("_metadata.file_path").alias("file_path"),
            current_timestamp().alias("ingestion_date"),
            col("_metadata.file_modification_time").alias("update_date")
        )
    )


@dp.table(
    name=f"{CATALOG}.bronze.bronze_transactions"
)
def bronze_transactions():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"/Volumes/{CATALOG}/bronze/raw/transactions/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"/Volumes/{CATALOG}/bronze/raw/transactions")
        .select(
            "*",
            col("_metadata.file_name").alias("file_name"),
            col("_metadata.file_path").alias("file_path"),
            current_timestamp().alias("ingestion_date"),
            col("_metadata.file_modification_time").alias("update_date")
        )
    )

@dp.table(
    name=f"{CATALOG}.bronze.bronze_branches"
)
def bronze_branches():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"/Volumes/{CATALOG}/bronze/raw/branches/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"/Volumes/{CATALOG}/bronze/raw/branches")
        .select(
            "*",
            col("_metadata.file_name").alias("file_name"),
            col("_metadata.file_path").alias("file_path"),
            current_timestamp().alias("ingestion_date"),
            col("_metadata.file_modification_time").alias("update_date")
        )
    )
