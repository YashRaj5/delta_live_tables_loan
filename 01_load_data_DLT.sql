-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Our Delta Live Table pipeline
-- MAGIC
-- MAGIC We'll be using as input a raw dataset containing information on our customers Loan and historical transactions. 
-- MAGIC
-- MAGIC Our goal is to ingest this data in near real time and build table for our Analyst team while ensuring data quality.
-- MAGIC
-- MAGIC Our datasets are coming from 3 different systems and saved under a cloud storage folder (S3/ADLS/GCS): 
-- MAGIC
-- MAGIC * `loans/raw_transactions` (loans uploader here in every few minutes)
-- MAGIC * `loans/ref_accounting_treatment` (reference table, mostly static)
-- MAGIC * `loans/historical_loans` (loan from legacy system, new data added every week)
-- MAGIC
-- MAGIC Let's ingest this data incrementally, and then compute a couple of aggregates that we'll need for our final Dashboard to report our KPI.

-- COMMAND ----------

-- DBTITLE 1,Let's review the incoming data
-- MAGIC %fs ls /tmp/dlt_loans/raw_transactions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze layer: incrementally ingest data leveraging Databricks Autoloader
-- MAGIC Our raw data is being sent to a blob storage.
-- MAGIC
-- MAGIC Autoloader simplify this ingestion, including schema inference, schema evolution while being able to scale to millions of incoming files.
-- MAGIC
-- MAGIC Autoloader is available in SQL using the cloud_files function and can be used with a variety of format (json, csv, avro...):
-- MAGIC
-- MAGIC ###STREAMING LIVE TABLE
-- MAGIC Defining tables as STREAMING will guarantee that you only consume new incoming data. Without STREAMING, you will scan and ingest all the data available at once.

-- COMMAND ----------

-- DBTITLE 1,Capture new incoming transaction data
CREATE STREAMING LIVE TABLE raw_txs
  COMMENT "New raw loan data incrementally ingested from cloud object storage landing zone"
AS SELECT * FROM cloud_files('/tmp/dlt_loans/raw_transactions', 'json', map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- import dlt
-- from pyspark.sql import functions as F

-- @dlt.create_table(comment="New raw loan data incrementally ingested from cloud object storage landing zone")
-- def raw_txs():
--   return (
--     spark.readStream.format("cloudFiles")
--       .option("cloudFiles.format", "json")
--       .option("cloudFiles.inferColumnTypes", "true")
--       .load("/demos/dlt/loans/raw_transactions"))

-- COMMAND ----------

-- DBTITLE 1,Reference table - metadata (small & static)
CREATE LIVE TABLE ref_accounting_treatment
  COMMENT "Lookup mapping for accounting codes"
AS SELECT * FROM delta.`/tmp/dlt_loans/ref_accounting_treatment`

-- COMMAND ----------

-- @dlt.create_table(comment="Lookup mapping for accounting codes")
-- def ref_accounting_treatment():
--   return spark.read.format("delta").load("/demos/dlt/loans/ref_accounting_treatment")

-- COMMAND ----------

-- DBTITLE 1,Historical transaction from legacy system
-- as this is only refreshed at a weekly basis, we can lower the interval
CREATE STREAMING LIVE TABLE raw_historical_loans
  TBLPROPERTIES ("pipelines.trigger.interval"="6 hour")
  COMMENT "Raw historical transactions"
AS SELECT * FROM cloud_files('/tmp/dlt_loans/historical_loans', 'csv', map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- @dlt.create_table(comment="Raw historical transactions")
-- def raw_historical_loans():
--   return (
--     spark.readStream.format("cloudFiles")
--       .option("cloudFiles.format", "csv")
--       .option("cloudFiles.inferColumnTypes", "true")
--       .load("/demos/dlt/loans/historical_loans"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver layer: joining tables while ensuring data quality
-- MAGIC
-- MAGIC Once the bronze layer is defined, we'll create the sliver layers by Joining data. Note that bronze tables are referenced using the LIVE spacename.
-- MAGIC
-- MAGIC To consume only increment from the Bronze layer like BZ_raw_txs, we'll be using the stream keyworkd: stream(LIVE.BZ_raw_txs)
-- MAGIC
-- MAGIC Note that we don't have to worry about compactions, DLT handles that for us.
-- MAGIC
-- MAGIC ### Expectations
-- MAGIC By defining expectations (CONSTRAINT EXPECT ), you can enforce and track your data quality. 

-- COMMAND ----------

-- DBTITLE 1,enrich transactions with metadata
CREATE STREAMING LIVE VIEW new_txs 
  COMMENT "Livestream of new transactions"
AS SELECT txs.*, ref.accounting_treatment as accounting_treatment FROM stream(LIVE.raw_txs) txs
  INNER JOIN live.ref_accounting_treatment ref ON txs.accounting_treatment_id = ref.id

-- COMMAND ----------

-- @dlt.create_view(comment="Livestream of new transactions")
-- def new_txs():
--   txs = dlt.read_stream("raw_txs").alias("txs")
--   ref = dlt.read("ref_accounting_treatment").alias("ref")
--   return (
--     txs.join(ref, F.col("txs.accounting_treatment_id") == F.col("ref.id"), "inner")
--       .selectExpr("txs.*", "ref.accounting_treatment as accounting_treatment"))

-- COMMAND ----------

-- DBTITLE 1,keep only proper transactions
CREATE STREAMING LIVE TABLE cleaned_new_txs (
  CONSTRAINT `Payments should be this year`  EXPECT (next_payment_date > date('2020-12-31')),
  CONSTRAINT `Balance should be positive`    EXPECT (balance > 0 AND arrears_balance > 0) ON VIOLATION DROP ROW,
  CONSTRAINT `Cost center must be specified` EXPECT (cost_center_code IS NOT NULL) ON VIOLATION FAIL UPDATE
)
  COMMENT "Livestream of new transactions, cleaned and compliant"
AS SELECT * from STREAM(live.new_txs)

-- COMMAND ----------

-- @dlt.create_table(comment="Livestream of new transactions, cleaned and compliant")
-- @dlt.expect("Payments should be this year", "(next_payment_date > date('2020-12-31'))")
-- @dlt.expect_or_drop("Balance should be positive", "(balance > 0 AND arrears_balance > 0)")
-- @dlt.expect_or_fail("Cost center must be specified", "(cost_center_code IS NOT NULL)")
-- def cleaned_new_txs():
--   return dlt.read_stream("new_txs")

-- COMMAND ----------

-- DBTITLE 1,Lets quarantine the bad transaction
-- This is the inverse condition of the above statement to quarantine incorrect data for further analysis.
CREATE STREAMING LIVE TABLE quarantine_bad_txs (
  CONSTRAINT `Payments should be this year`  EXPECT (next_payment_date <= date('2020-12-31')),
  CONSTRAINT `Balance should be positive`    EXPECT (balance <= 0 OR arrears_balance <= 0) ON VIOLATION DROP ROW
)
  COMMENT "Incorrect transactions requiring human analysis"
AS SELECT * from STREAM(live.new_txs)

-- COMMAND ----------

-- #This is the inverse condition of the above statement to quarantine incorrect data for further analysis.
-- @dlt.create_table(comment="Incorrect transactions requiring human analysis")
-- @dlt.expect("Payments should be this year", "(next_payment_date <= date('2020-12-31'))")
-- @dlt.expect_or_drop("Balance should be positive", "(balance <= 0 OR arrears_balance <= 0)")
-- def quarantine_bad_txs():
--   return dlt.read_stream("new_txs")

-- COMMAND ----------

-- DBTITLE 1,Enrich all historical transactions
CREATE LIVE TABLE historical_txs
  COMMENT "Historical loan transactions"
AS SELECT l.*, ref.accounting_treatment as accounting_treatment FROM LIVE.raw_historical_loans l
  INNER JOIN LIVE.ref_accounting_treatment ref ON l.accounting_treatment_id = ref.id

-- COMMAND ----------

-- @dlt.create_table(comment="Historical loan transactions")
-- @dlt.expect("Grade should be valid", "(grade in ('A', 'B', 'C', 'D', 'E', 'F', 'G'))")
-- @dlt.expect_or_drop("Recoveries shoud be int", "(CAST(recoveries as INT) IS NOT NULL)")
-- def historical_txs():
--   history = dlt.read_stream("raw_historical_loans").alias("l")
--   ref = dlt.read("ref_accounting_treatment").alias("ref")
--   return (history.join(ref, F.col("l.accounting_treatment_id") == F.col("ref.id"), "inner") 
--                  .selectExpr("l.*", "ref.accounting_treatment as accounting_treatment"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Layer
-- MAGIC
-- MAGIC Our last step is to materialize the Gold Layer.
-- MAGIC
-- MAGIC Because these tables will be requested at scale using a SQL Endpoint, we'll add Zorder at the table level to ensure faster queries using pipelines.autoOptimize.zOrderCols, and DLT will handle the rest.

-- COMMAND ----------

-- DBTITLE 1,Balance aggregate per cost location
CREATE LIVE TABLE total_loan_balances
  COMMENT "Combines historical and new loan data for unified rollup of loan balances"
  TBLPROPERTIES ("pipelines.autoOptimize.zOrderCols" = "location_code")
AS SELECT sum(revol_bal)  AS bal, addr_state   AS location_code FROM live.historical_txs  GROUP BY addr_state
  UNION SELECT sum(balance) AS bal, country_code AS location_code FROM live.cleaned_new_txs GROUP BY country_code

-- COMMAND ----------

-- @dlt.create_table(
--   comment="Combines historical and new loan data for unified rollup of loan balances",
--   table_properties={"pipelines.autoOptimize.zOrderCols": "location_code"})
-- def total_loan_balances():
--   return (
--     dlt.read("historical_txs")
--       .groupBy("addr_state")
--       .agg(F.sum("revol_bal").alias("bal"))
--       .withColumnRenamed("addr_state", "location_code")
--       .union(
--         dlt.read("cleaned_new_txs")
--           .groupBy("country_code")
--           .agg(F.sum("balance").alias("bal"))
--           .withColumnRenamed("country_code", "location_code")
--       )          
--   )

-- COMMAND ----------

-- DBTITLE 1,Balance aggregate per cost center
CREATE LIVE TABLE new_loan_balances_by_cost_center
  COMMENT "Live table of new loan balances for consumption by different cost centers"
AS SELECT sum(balance) as sum_balance, cost_center_code FROM live.cleaned_new_txs
  GROUP BY cost_center_code

-- COMMAND ----------

-- @dlt.create_table(
--   comment="Live table of new loan balances for consumption by different cost centers")
-- def new_loan_balances_by_cost_center():
--   return (
--     dlt.read("cleaned_new_txs")
--       .groupBy("cost_center_code")
--       .agg(F.sum("balance").alias("sum_balance"))
--   )

-- COMMAND ----------

-- DBTITLE 1,Balance aggregate per country
CREATE LIVE TABLE new_loan_balances_by_country
  COMMENT "Live table of new loan balances per country"
AS SELECT sum(count) as sum_count, country_code FROM live.cleaned_new_txs GROUP BY country_code

-- COMMAND ----------

-- @dlt.create_table(
--   comment="Live table of new loan balances per country")
-- def new_loan_balances_by_country():
--   return (
--     dlt.read("cleaned_new_txs")
--       .groupBy("country_code")
--       .agg(F.sum("count").alias("sum_count"))
--   )
