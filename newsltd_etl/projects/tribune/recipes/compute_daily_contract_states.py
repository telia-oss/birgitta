# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# # Create fact table

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
## Initializing necessary modules related to dataiku & pyspark
import datetime

from birgitta import spark as bspark
from birgitta.dataframe import dataframe
from birgitta.recipe import params as recipe_params
from birgitta.recipe.debug import dataframe as dfdbg
from examples.organizations.newsltd.projects.tribune.datasets.daily_contract_states import dataset as ds_daily_contract_states # noqa 501
from examples.organizations.newsltd.projects.tribune.datasets.filtered_contracts import dataset as ds_filtered_contracts # noqa 501
from pyspark.sql import functions as f
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
analysis_start_date = datetime.date(2016, 1, 1)
today_date = recipe_params.today()
spark_session = bspark.session()
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
filtered_contracts = dataframe.get(spark_session,
                              ds_filtered_contracts.name,
                              cast_binary_to_str=True)
datedim = dataframe.get(spark_session, "date_dim")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
sel_cols = [
         'customer_id',
         'product_code',
         'product',
         'segment',
         'product_name',
         'brand_name',
         'start_date',
         'end_date',
         'shop_code',
         'product_category']

contracts = filtered_contracts.select(*sel_cols)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
## Convert timestamps to dates
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
contracts = contracts.withColumn("start_date",
                       f.col("start_date").cast('date')) \
       .withColumn("end_date", f.col("end_date").cast('date'))
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# ## Adjust start and end dates
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# select only if expiration date is after analysis_start_date
contracts = contracts.filter(contracts.end_date >= analysis_start_date)

# Adjust start_date to be equal to analysis_start_date,
# if it is before the analysis_start_date
start_when = f.when(f.col("start_date") < analysis_start_date,
                    analysis_start_date) \
              .otherwise(f.col("start_date"))
contracts = contracts.withColumn('start_date_adj', start_when)
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Adjust end_date to be equal to today, if it is after today
start_when = f.when(f.col("end_date") > today_date,
                    today_date) \
              .otherwise(f.col("end_date"))
contracts = contracts.withColumn('end_date_adj', start_when)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: MARKDOWN
# # Join with date_dimension, thereby splitting into daily granularity
# This will split into days.

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
t_dim_days = datedim.select(f.col('datetimestamp_parsed').alias("datestamp"),
                            "day_in_week") \
               .withColumn("datestamp", f.col("datestamp").cast('date'))

join_conditions = [(contracts.start_date_adj <= t_dim_days.datestamp)
                   & (contracts.end_date_adj >= t_dim_days.datestamp)]

contracts_daily = contracts.join(
    t_dim_days,
    join_conditions,
    'left'
).drop('start_date_adj').drop('end_date_adj')

# Debug contracts_daily with our utility functions
dfdbg.profile(contracts_daily, "contracts_name")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Write recipe outputs
dataframe.write(contracts_daily,
                ds_daily_contract_states.name,
                schema=ds_daily_contract_states.schema)
