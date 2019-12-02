# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# # Compute filtered_contracts
#
# Documentation goes here...

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
from birgitta import spark as bspark
from birgitta.dataframe import dataframe
from pyspark.sql import functions as F
from newsltd_etl.projects.tribune.datasets.filtered_contracts import dataset as ds_filtered_contracts # noqa 501
from newsltd_etl.projects.tribune.datasets.contracts import dataset as ds_contracts # noqa 501

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#  Get or create sparkcontext and set up sqlcontext
spark_session = bspark.session()
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
contracts = dataframe.get(spark_session,
                     ds_contracts.name,
                     cast_binary_to_str=True)
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ## Convert timestamps to dates

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE

contractsf = contracts.filter(contracts.brand_code != 44) \
        .withColumnRenamed("contract_prod_code", "product_code") \
        .withColumn("start_date", F.col("start_date").cast('date')) \
        .withColumn("end_date", F.col("end_date").cast('date'))

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN

# ## Add product category
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
## Add "product_category"
contractsf = contractsf.withColumn("product_category", F.lit("contract"))
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ## Write to output data

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
dataframe.write(contractsf,
                ds_filtered_contracts.name,
                schema=ds_filtered_contracts.schema)
