# ------- NOTEBOOK-CELL: MARKDOWN
# Starting comment
import pyspark
from pyspark.sql import SQLContext

# ------- NOTEBOOK-CELL: CODE
#  Get or create sparkcontext and set up sqlcontext
sc = pyspark.SparkContext.getOrCreate()
log_transform(sc, 8, "sc = pyspark.SparkContext.getOrCreate()")
sql_context = SQLContext(sc)
log_transform(sql_context, 9, "sql_context = SQLContext(sc)")

write_cols = [
    'datestamp',
#     'sequence_no',
#     'phone',
    'customer_id'
]


# ------- NOTEBOOK-CELL: CODE
use_test_examples = False
log_transform(use_test_examples, 20, "use_test_examples = False")
test_customer_ids = ["9493790"]
# test_customer_ids = ['5736']
test_customer_ids = ['16191188']
co = co\
    .filter("client != 1234")
log_transform(co, 24, "co = co")

co2 = co \
    .filter("client != 1235")
log_transform(co2, 27, "co2 = co ")

co3 = co2 \ 
    .filter("client != 1236")
log_transform(co3, 30, "co3 = co2  ")
# ------- NOTEBOOK-CELL: CODE
dataframe.write(commit_out, "commit_hist")
foo = commit_out
log_transform(foo, 34, "foo = commit_out")
foo2 = commit_out
log_transform(foo2, 35, "foo2 = commit_out")