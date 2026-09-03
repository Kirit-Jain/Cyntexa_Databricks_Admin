from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

CATALOG = spark.conf.get("catalog")


@dp.table(
    name=f"{CATALOG}.gold.gold_dim_customer",
    comment="Customer dimension with complete profile for analytics"
)
def gold_dim_customer():
    return (
        spark.readStream.table(f"{CATALOG}.silver.silver_customers")
        .select(
            col("customer_id"),
            concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"),
            when(expr("is_account_group_member('DATA ENGINEER')"), col("email"))
                .otherwise(concat(substring(col("email"), 1, 2), lit("***@"), regexp_extract(col("email"), r"@(.+)$", 1))).alias("email"),
            when(expr("is_account_group_member('DATA ENGINEER')"), col("phone"))
                .otherwise(concat(lit("******"), substring(col("phone"), -4, 4))).alias("phone"),
            col("address"),
            col("date_of_birth"),
            current_timestamp().alias("dim_updated_at")
        )
    )


@dp.table(
    name=f"{CATALOG}.gold.gold_dim_branch",
    comment="Branch dimension with geographic and operational attributes"
)
def gold_dim_branch():
    """
    Branch dimension table with location hierarchy.
    Source: silver_branches
    """
    return (
        spark.readStream.table(f"{CATALOG}.silver.silver_branches")
        .select(
            col("branch_id"),
            col("branch_name"),
            col("city"),
            col("state"),
            col("region"),
            col("branch_type"),
            col("branch_status"),
            col("opening_date"),
            datediff(current_date(), col("opening_date")).alias("days_since_opening"),
            current_timestamp().alias("dim_updated_at")
        )
    )


@dp.table(
    name=f"{CATALOG}.gold.gold_dim_account",
    comment="Account dimension with product and status attributes"
)
def gold_dim_account():
    return (
        spark.readStream.table(f"{CATALOG}.silver.silver_accounts")
        .select(
            col("account_id"),
            col("customer_id"),
            col("branch_id"),
            col("account_type"),
            col("account_status"),
            col("opening_date"),
            col("closing_date"),
            col("currency"),
            col("account_tier"),
            col("interest_rate"),
            when(col("account_status") == "Active", 
                 datediff(current_date(), col("opening_date")))
            .otherwise(datediff(col("closing_date"), col("opening_date")))
            .alias("account_age_days"),
            when(col("account_status") == "Active", lit(True))
            .otherwise(lit(False)).alias("is_active"),
            current_timestamp().alias("dim_updated_at")
        )
    )


#### FACT TABLE ####

@dp.table(
    name=f"{CATALOG}.gold.gold_fact_transactions",
    comment="Transaction fact table with dimensional keys and measures",
    partition_cols=["transaction_date"]
)
def gold_fact_transactions():
    transactions = spark.readStream.table(f"{CATALOG}.silver.silver_transactions")
    
    # Read dimensions for enrichment (batch reads for stream-static joins)
    accounts = spark.read.table(f"{CATALOG}.silver.silver_accounts").select(
        col("account_id"),
        col("customer_id"),
        col("branch_id"),
        col("account_type")
    )
    
    return (
        transactions
        .join(accounts, "account_id", "left")
        .select(
            col("transaction_id"),
            col("account_id"),
            col("customer_id"),
            col("branch_id"),
            
            col("transaction_type"),
            col("transaction_channel"),
            col("transaction_status"),
            col("merchant_name"),
            col("merchant_category"),
            col("reference_number"),
            
            col("amount"),
            col("currency"),
            
            col("transaction_date"),
            col("transaction_timestamp"),
            year(col("transaction_date")).alias("transaction_year"),
            quarter(col("transaction_date")).alias("transaction_quarter"),
            month(col("transaction_date")).alias("transaction_month"),
            dayofweek(col("transaction_date")).alias("transaction_day_of_week"),
            hour(col("transaction_timestamp")).alias("transaction_hour"),
            
            col("account_type"),
            
            when(col("transaction_status") == "Completed", lit(1)).otherwise(lit(0)).alias("is_successful"),
            when(col("transaction_status") == "Failed", lit(1)).otherwise(lit(0)).alias("is_failed"),
            
            current_timestamp().alias("fact_loaded_at")
        )
    )










# +++++++++++++++ KPI +++++++++++++++++
@dp.materialized_view(
    name=f"{CATALOG}.gold.kpi_total_transaction_value"
)
def kpi_total_transaction_value():
    return (
        spark.read.table(f"{CATALOG}.gold.gold_fact_transactions")
        .filter(col("is_successful")==1)
        .agg(
            sum(col("amount")).alias("total_transaction_value"),
            avg(col("amount")).alias("avg_transaction_value"),
            max(col("amount")).alias("max_transaction_value"),
            min(col("amount")).alias("min_transaction_value"),
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.kpi_total_transaction_count"
)
def kpi_total_transaction_count():
    return(
        spark.read.table(f"{CATALOG}.gold.gold_fact_transactions")
        .filter(col("is_successful")==1)
        .agg(
            count(col("transaction_id")).alias("total_successful_transaction")
        )
    )

@dp.materialized_view(
    name=f"{CATALOG}.gold.kpi_account_outflow_inflow"
)
def kpi_account_outflow_inflow():
    return(
        spark.read.table(f"{CATALOG}.gold.gold_fact_transactions")
        .groupBy("account_id")
        .agg(
            sum(
                when(col("transaction_type").isin("Deposit", "Interest Credit"), col("amount")).otherwise(0)
            ).alias("total_inflow"),
            sum(
                when(col("transaction_type").isin("Withdrawal", "Payment", "Fee"), col("amount")).otherwise(0)
            ).alias("total_outflow"),
            sum(
                when(col("transaction_type").isin("Transfer"), col("amount")).otherwise(0)
            ).alias("total_transfer")
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.kpi_transaction_by_month"
)
def kpi_transaction_by_month():
    return (
        spark.read.table(f"{CATALOG}.gold.gold_fact_transactions")
        .groupBy("transaction_month")
        .agg(
            count(col("transaction_id")).alias("total_transaction"),
            sum(col("amount")).alias("total_transaction_value"),
            avg(col("amount")).alias("avg_transaction_value"),
            max(col("amount")).alias("max_transaction_value"),
            min(col("amount")).alias("min_transaction_value"),
        )
    )