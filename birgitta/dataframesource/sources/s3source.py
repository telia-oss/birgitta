"""The S3 dataframesource class.
"""
from birgitta.dataframesource import DataframeSourceBase

__all__ = ['S3Source']


class S3Source(DataframeSourceBase):
    """Instantiate an S3Source object, defining the format, e.g. 'json'
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if 'format' in kwargs:
            self.format = kwargs['format']
        else:
            self.format = 'parquet'

    def load(self, spark_session, dataset_name, prefix, **kwargs):
        s3_path = prefix  # Assume that prefix is a valid s3_path
        dest_path = f"s3a://{s3_path}/{dataset_name}.{self.format}"
        schema = kwargs.get('schema')
        read = spark_session.read
        if schema:
            read = read.schema(schema.to_spark())
        return read.format(self.format).load(dest_path)

    def write(self, df, dataset_name, prefix, **kwargs):
        s3_path = prefix  # Assume that prefix is a valid s3_path
        dest_path = f"s3a://{s3_path}/{dataset_name}.{self.format}"
        df.write.format(self.format).mode("overwrite").save(dest_path)
