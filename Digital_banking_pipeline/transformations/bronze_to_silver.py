from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, trim, regexp_replace, ltrim, rtrim, current_timestamp,
    when, lower, upper, initcap, to_date, length,
    lit, coalesce, to_timestamp
)

CATALOG = spark.conf.get("catalog")

#### Customer Silver ####

@dp.temporary_view(
    name="silver_customers_stg"
)
def silver_customers_stg():
    df = spark.readStream.table(f"{CATALOG}.bronze.bronze_customers")
    
    # Apply cleaning transformations
    cleaned_df = (
        df
        .withColumn("first_name", initcap(trim(col("first_name"))))
        .withColumn("last_name", initcap(trim(col("last_name"))))
        .withColumn("email", lower(trim(col("email"))))
        
        
        .withColumn("phone", trim(col("phone")))
        .withColumn("address", trim(col("address")))
        
        .withColumn("date_of_birth", 
                   when(col("date_of_birth").isNotNull(), 
                        to_date(col("date_of_birth"), "yyyy-MM-dd"))
                   .otherwise(None))
        
        .withColumn("is_valid_customer_id", 
                   col("customer_id").isNotNull() & (length(col("customer_id")) > 0))
        
        .withColumn("is_valid_email", 
                   col("email").isNotNull() & 
                   col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"))
        
        .withColumn("is_valid_name", 
                   col("first_name").isNotNull() & 
                   col("last_name").isNotNull() &
                   (length(col("first_name")) > 0) &
                   (length(col("last_name")) > 0))
        
        .withColumn("is_valid_date", 
                   col("date_of_birth").isNull() |
                   ((col("date_of_birth") >= lit("1900-01-01")) & 
                    (col("date_of_birth") <= current_timestamp())))

        .withColumn("is_valid_record",
                   col("is_valid_customer_id") &
                   col("is_valid_email") &
                   col("is_valid_name") &
                   col("is_valid_date"))
        
        .withColumn("quarantine_reason",
                   when(~col("is_valid_customer_id"), "Invalid or missing customer_id")
                   .when(~col("is_valid_email"), "Invalid email format")
                   .when(~col("is_valid_name"), "Missing or invalid name")
                   .when(~col("is_valid_date"), "Invalid date of birth")
                   .otherwise(None))
    )
    
    # Deduplicate by customer_id (keeps arbitrary record per key within each batch)
    deduplicated_df = cleaned_df.dropDuplicates(["customer_id"])
    
    return deduplicated_df


@dp.temporary_view(
    name="silver_customers_cleaned_stg"
)
def silver_customers_cleaned_stg():
    return (
        spark.readStream.table("silver_customers_stg")
        .filter(col("is_valid_record") == True)
        .drop("is_valid_customer_id", "is_valid_email", "is_valid_name", 
              "is_valid_date", "is_valid_record", "quarantine_reason")
    )

@dp.table(
    name=f"{CATALOG}.silver.silver_customers_quarantine",
    comment="Quarantined customer records that failed data quality validation - requires manual review",
)
def silver_customers_quarantine():
    return (
        spark.readStream.table("silver_customers_stg")
        .filter(col("is_valid_record") == False)
        .withColumn("quarantine_timestamp", current_timestamp())
    )

dp.create_streaming_table(
    name=f"{CATALOG}.silver.silver_customers",
)

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.silver_customers",
    source="silver_customers_cleaned_stg",
    keys=["customer_id"],
    sequence_by= col("ingestion_date"),
    except_column_list=["updated_at", "file_name", "file_path", "ingestion_date", "update_date"],
    stored_as_scd_type="1"
)

#### banches ####

@dp.temporary_view(
    name="silver_branches_stg"
)
def silver_branches_stg ():
    df = spark.readStream.table(f"{CATALOG}.bronze.bronze_branches")

    cleaned_df = (
        df
            .withColumn("branch_name", initcap(trim(col('branch_name'))))
            .withColumn("city", initcap(trim(col("city"))))
            .withColumn("state", initcap(trim(col("state"))))
            .withColumn("region", initcap(trim(col("region"))))
            .withColumn("branch_status", initcap(trim(col("branch_status"))))

            .withColumn("opening_date", 
                   when(col("opening_date").isNotNull(), 
                        to_date(col("opening_date"), "yyyy-MM-dd"))
                   .otherwise(None))
            
            .withColumn("is_valid_branch_id", 
                   col("branch_id").isNotNull() & (length(col("branch_id")) > 0))
            
            .withColumn("is_valid_branch_type", 
                   col("branch_type").isNotNull() & (length(col("branch_type")) > 0) & col("branch_type").isin("Metro", "Semi-Urban", "Urban", "Rural"))
            
            .withColumn("is_valid_date", 
                   col("opening_date").isNull() |
                   ((col("opening_date") >= lit("1900-01-01")) & 
                    (col("opening_date") <= current_timestamp())))
            
            .withColumn("is_valid_record",
                   col("is_valid_branch_id") &
                   col("is_valid_branch_type") &
                   col("is_valid_date"))
        
            .withColumn("quarantine_reason",
                   when(~col("is_valid_branch_id"), "Invalid or missing branch_id")
                   .when(~col("is_valid_branch_type"), "Invalid branch type")
                   .when(~col("is_valid_date"), "Invalid date of birth")
                   .otherwise(None))
    )
    
    # Deduplicate by branch_id (keeps arbitrary record per key within each batch)
    deduplicated_df = cleaned_df.dropDuplicates(["branch_id"])
    
    return deduplicated_df

@dp.table(
    name=f"{CATALOG}.silver.silver_branches"
)
def silver_branches():
    return (
        spark.readStream.table("silver_branches_stg")
        .filter(col("is_valid_record")==True)
        .drop("file_name", "file_path", "ingestion_date", "update_date","is_valid_branch_id", "is_valid_branch_type", "is_valid_date", "is_valid_record", "quarantine_reason")
    )

@dp.table(
    name=f"{CATALOG}.silver.silver_branches_quarantine",
    comment="Quarantined customer records that failed data quality validation - requires manual review",
)
def silver_branches_quarantine():
    return (
        spark.readStream.table("silver_branches_stg")
        .filter(col("is_valid_record")==False)
        .withColumn("quarantine_timestamp", current_timestamp())
    )

#### Accounts ####

@dp.temporary_view(
    name="silver_accounts_stg"
)
def silver_accounts_stg():
    # Read accounts as a stream
    df = spark.readStream.table(f"{CATALOG}.bronze.bronze_accounts")
    
    valid_customers = (
        spark.read.table(f"{CATALOG}.silver.silver_customers")
        .select(col("customer_id").alias("valid_customer_id"))
        .distinct()
    )
    
    valid_branches = (
        spark.read.table(f"{CATALOG}.silver.silver_branches")
        .select(col("branch_id").alias("valid_branch_id"))
        .distinct()
    )
    
    cleaned_df = (
        df
        .withColumn("account_id", trim(col("account_id")))
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("branch_id", trim(col("branch_id")))
        .withColumn("account_type", initcap(trim(col("account_type"))))
        .withColumn("account_status", initcap(trim(col("account_status"))))
        .withColumn("currency", upper(trim(col("currency"))))
        .withColumn("account_tier", initcap(trim(col("account_tier"))))
        
        .withColumn("opening_date", 
                   when(col("opening_date").isNotNull(), 
                        to_date(col("opening_date"), "yyyy-MM-dd"))
                   .otherwise(None))
        .withColumn("closing_date", 
                   when(col("closing_date").isNotNull(), 
                        to_date(col("closing_date"), "yyyy-MM-dd"))
                   .otherwise(None))
        .withColumn("interest_rate", col("interest_rate").cast("double"))

        .join(valid_customers, col("customer_id") == col("valid_customer_id"), "left")
        .withColumn("is_valid_customer", col("valid_customer_id").isNotNull())
        .drop("valid_customer_id")
    
        .join(valid_branches, col("branch_id") == col("valid_branch_id"), "left")
        .withColumn("is_valid_branch", col("valid_branch_id").isNotNull())
        .drop("valid_branch_id")
        
        .withColumn("is_valid_account_type", 
                   col("account_type").isin("Savings", "Current", "Salary", "Business"))
        
        .withColumn("is_valid_account_status", 
                   col("account_status").isin("Active", "Dormant", "Closed"))
        
        .withColumn("is_valid_dates",
                   col("opening_date").isNotNull() &
                   (col("opening_date") <= current_timestamp()) &
                   (col("closing_date").isNull() | (col("closing_date") >= col("opening_date"))))
        
        .withColumn("is_valid_status_date",
                   when(col("account_status") == "Closed", col("closing_date").isNotNull())
                   .when(col("account_status") == "Active", col("closing_date").isNull())
                   .otherwise(lit(True)))
        
        .withColumn("is_valid_record",
                   col("is_valid_customer") &
                   col("is_valid_branch") &
                   col("is_valid_account_type") &
                   col("is_valid_account_status") &
                   col("is_valid_dates") &
                   col("is_valid_status_date"))
        
        .withColumn("quarantine_reason",
                   when(~col("is_valid_customer"), "Customer ID not found in customers table")
                   .when(~col("is_valid_branch"), "Branch ID not found in branches table")
                   .when(~col("is_valid_account_type"), "Invalid account type")
                   .when(~col("is_valid_account_status"), "Invalid account status")
                   .when(~col("is_valid_dates"), "Invalid account dates (opening/closing date)")
                   .when(~col("is_valid_status_date"), "Account status and closing date mismatch")
                   .otherwise(None))
    )
    
    # Deduplicate by account_id (keeps arbitrary record per key within each batch)
    deduplicated_df = cleaned_df.dropDuplicates(["account_id"])
    
    return deduplicated_df


@dp.table(
    name=f"{CATALOG}.silver.silver_accounts",
    comment="Clean, validated account records with data quality and relationship checks applied"
)
def silver_accounts():
    return (
        spark.readStream.table("silver_accounts_stg")
        .filter(col("is_valid_record") == True)
        .drop("file_name", "file_path", "ingestion_date", "update_date", "is_valid_customer", "is_valid_branch", "is_valid_account_type",
              "is_valid_account_status", "is_valid_dates", "is_valid_status_date",
              "is_valid_record", "quarantine_reason")
    )

@dp.table(
    name=f"{CATALOG}.silver.silver_accounts_quarantine",
    comment="Quarantined account records that failed data quality or relationship validation - requires manual review",
)
def silver_accounts_quarantine():
    return (
        spark.readStream.table("silver_accounts_stg")
        .filter(col("is_valid_record") == False)
        .withColumn("quarantine_timestamp", current_timestamp())
    )



#### Transactions ####

@dp.temporary_view(
    name="silver_transactions_stg"
)
def silver_transactions_stg():
    df = spark.readStream.table(f"{CATALOG}.bronze.bronze_transactions")
    
    # Read valid account_ids from silver_accounts (batch/static for stream-static join)
    valid_accounts = (
        spark.read.table(f"{CATALOG}.silver.silver_accounts")
        .select(col("account_id").alias("valid_account_id"))
        .distinct()
    )
    
    cleaned_df = (
        df
        .withColumn("transaction_id", trim(col("transaction_id")))
        .withColumn("account_id", trim(col("account_id")))
        .withColumn("transaction_type", initcap(trim(col("transaction_type"))))
        .withColumn("transaction_channel", trim(col("transaction_channel")))
        .withColumn("currency", upper(trim(col("currency"))))
        .withColumn("merchant_name", trim(col("merchant_name")))
        .withColumn("merchant_category", initcap(trim(col("merchant_category"))))
        .withColumn("transaction_status", initcap(trim(col("transaction_status"))))
        .withColumn("reference_number", trim(col("reference_number")))
        
        # Cast to correct datatypes
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("transaction_date", 
                   when(col("transaction_date").isNotNull(), 
                        to_date(col("transaction_date"), "yyyy-MM-dd"))
                   .otherwise(None))
        .withColumn("transaction_timestamp", 
                   when(col("transaction_timestamp").isNotNull(), 
                        to_timestamp(col("transaction_timestamp")))
                   .otherwise(None))
        .withColumn("created_at", 
                   when(col("created_at").isNotNull(), 
                        to_timestamp(col("created_at")))
                   .otherwise(None))
        .withColumn("amount", col("amount").cast("double"))

        .join(valid_accounts, col("account_id") == col("valid_account_id"), "left")
        .withColumn("is_valid_account", col("valid_account_id").isNotNull())
        .drop("valid_account_id")
        
        .withColumn("is_valid_transaction_type", 
                   col("transaction_type").isin("Payment", "Deposit", "Transfer", "Withdrawal", "Fee", "Interest Credit"))
        
        # Validate transaction status
        .withColumn("is_valid_transaction_status", 
                   col("transaction_status").isin("Completed", "Failed", "Reversed"))
        
        # Validate transaction channel (ATM must stay uppercase, so no initcap)
        .withColumn("is_valid_channel", 
                   col("transaction_channel").isin("Mobile", "Internet Banking", "Branch", "ATM", "System"))
        
        # Validate amount
        .withColumn("is_valid_amount", 
                   col("amount").isNotNull() & (col("amount") > 0))
        
        # Validate dates
        .withColumn("is_valid_dates",
                   col("transaction_date").isNotNull() &
                   (col("transaction_date") <= current_timestamp()) &
                   col("transaction_timestamp").isNotNull())
        
        # Combined validity
        .withColumn("is_valid_record",
                   col("is_valid_account") &
                   col("is_valid_transaction_type") &
                   col("is_valid_transaction_status") &
                   col("is_valid_channel") &
                   col("is_valid_amount") &
                   col("is_valid_dates"))
        
        # Quarantine reason
        .withColumn("quarantine_reason",
                   when(~col("is_valid_account"), "Account ID not found in accounts table")
                   .when(~col("is_valid_transaction_type"), "Invalid transaction type")
                   .when(~col("is_valid_transaction_status"), "Invalid transaction status")
                   .when(~col("is_valid_channel"), "Invalid transaction channel")
                   .when(~col("is_valid_amount"), "Invalid or missing amount")
                   .when(~col("is_valid_dates"), "Invalid transaction dates")
                   .otherwise(None))
    )
    
    # Deduplicate by transaction_id (keeps arbitrary record per key within each batch)
    deduplicated_df = cleaned_df.dropDuplicates(["transaction_id"])
    
    return deduplicated_df


@dp.table(
    name=f"{CATALOG}.silver.silver_transactions",
    comment="Clean, validated transaction records with data quality and relationship checks applied"
)
def silver_transactions():
    return (
        spark.readStream.table("silver_transactions_stg")
        .filter(col("is_valid_record") == True)
        .drop("file_name", "file_path", "ingestion_date", "update_date",
              "is_valid_account", "is_valid_transaction_type", "is_valid_transaction_status",
              "is_valid_channel", "is_valid_amount", "is_valid_dates",
              "is_valid_record", "quarantine_reason")
    )

@dp.table(
    name=f"{CATALOG}.silver.silver_transactions_quarantine",
    comment="Quarantined transaction records that failed data quality or relationship validation - requires manual review",
)
def silver_transactions_quarantine():
    return (
        spark.readStream.table("silver_transactions_stg")
        .filter(col("is_valid_record") == False)
        .withColumn("quarantine_timestamp", current_timestamp())
    )











