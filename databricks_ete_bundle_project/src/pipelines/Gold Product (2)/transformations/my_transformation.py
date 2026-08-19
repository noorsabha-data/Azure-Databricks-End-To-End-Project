import dlt
from pyspark.sql.functions import * 

# Expectations
my_rules = {
    "rule1" : "product_id IS NOT NULL",
    "rule2" : "product_name IS NOT NULL"
}

@dlt.table()

@dlt.expect_all_or_drop(my_rules)
def DimProducts_stage(): 

  df = spark.readStream.option("skipChangeCommits","true").table("databricks_catalog.silver.products_silver")

  return df

@dlt.view 

def DimProducts_view():

    df = spark.readStream.table("DimProducts_stage")
    return df 

dlt.create_streaming_table("DimProducts")

dlt.apply_changes(
  target = "DimProducts",
  source = "DimProducts_view",
  keys = ["product_id"],
  sequence_by = "product_id",
  stored_as_scd_type = 2
)