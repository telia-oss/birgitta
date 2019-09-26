# ------- NOTEBOOK-CELL: MARKDOWN
# Starting comment
import pyspark
from pyspark.sql import SQLContext

# ------- NOTEBOOK-CELL: CODE
#  Get or create sparkcontext and set up sqlcontext
sc = pyspark.SparkContext.getOrCreate()
sql_context = SQLContext(sc)

write_cols = [
    'datestamp',
#     'sequence_no',
#     'phone',
    'customer_id'
]


# ------- NOTEBOOK-CELL: CODE
use_test_examples = False
test_customer_ids = ["9493790"]
# test_customer_ids = ['5736']
test_customer_ids = ['16191188']
co = co\
    .filter("client != 1234")

co2 = co \
    .filter("client != 1235")

co3 = co2 \
    .filter("client != 1236")
# ------- NOTEBOOK-CELL: CODE
dataframe.write(commit_out, "commit_hist")
foo = commit_out
foo2 = commit_out