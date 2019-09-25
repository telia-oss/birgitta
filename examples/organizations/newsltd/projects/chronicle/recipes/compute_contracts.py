# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import warnings

from birgitta import spark as bspark
from birgitta.dataframe import dataframe
from examples.organizations.newsltd.projects.chronicle.datasets.contract_data import dataset as ds_contract_data  # noqa 501
from examples.organizations.newsltd.projects.chronicle.datasets.contracts import dataset as ds_contracts  # noqa 501
from pyspark.sql import functions as F
warnings.filterwarnings('ignore')  # supress python warnings


# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
sql_context = bspark.sql_ctx()
contract_data_df = dataframe.get(sql_context,
                                     ds_contract_data.name)

with_flag = contract_data_df.withColumn('current_flag', F.lit(1))

to_output_df = with_flag.select(
    F.col('customerid').alias('customer_id'),
    F.concat(F.lit('G47'), F.col('cellphone')).alias('phone'),
    F.col('accountid').alias('chronicle_account_id'),
    F.col('groupid').alias('group_account_id'),
    F.col('priceplan_code'),
    F.col('startdate_yyyymmdd').cast('date').alias('start_date'),
    F.col('enddate_yyyymmdd').cast('date').alias('end_date'),
    F.col('current_flag'),
    F.col('status').alias('client_status_code')
)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Write recipe outputs
dataframe.write(to_output_df,
                ds_contracts.name,
                schema=ds_contracts.schema)
