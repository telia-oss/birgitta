"""The Dataiku dataframesource class.
"""
import dataiku
import dataiku.spark as dkuspark
import dataikuapi
import pyspark
from birgitta.dataframesource import DataframeSourceBase
from birgitta.dataiku import schema as dkuschema
from pyspark.sql import SQLContext


__all__ = ['DataikuSource']


class DataikuSource(DataframeSourceBase):
    def load(self,
             spark_session,
             dataset_name,
             prefix,
             *,
             skip_cast=False,
             schema=None):
        """When prefix is None use same project as recipe is in."""
        project_key = prefix  # Assume that prefix is a valid project_key
        dataset = dataiku.Dataset(dataset_name, project_key)
        sql_ctx = self.dataiku_sql_ctx()
        df = dkuspark.get_dataframe(sql_ctx, dataset)
        if schema and not skip_cast:
            # FUTURE: implement native schema on read, if available on DSS
            df = schema.cast(df)
        return df

    def write(self,
              df,
              dataset_name,
              prefix,
              *,
              schema=None,
              set_schema=True):
        """When project_key is None use same project as recipe is in."""
        project_key = prefix  # Assume that prefix is a valid project_key
        dku_dataset = dataiku.Dataset(dataset_name, project_key)
        print(f"Writing {dataset_name} to dku in project {project_key}")
        # Use write_with_schema instead of write_dataframe, because we assume
        # type handling will be more robust.
        dkuspark.write_with_schema(dku_dataset, df)
        # Set schema after write_with_schema, to enforce types and add comments
        if schema and set_schema:
            self.set_schema(dataset_name,
                            dku_dataset,
                            schema,
                            project_key)

    def set_schema(self,
                   dataset_name,
                   dku_dataset,
                   schema,
                   project_key):
        dataiku_schema = dkuschema.to_dataiku(schema)
        client = dataiku.api_client()
        if not project_key:
            project_key = dku_dataset.get_config()['projectKey']
        project = dataikuapi.dss.project.DSSProject(client, project_key)
        dapi_dataset = project.get_dataset(dataset_name)
        ret = dapi_dataset.set_schema(dataiku_schema)
        print(f"dataset.set_schema() for {dataset_name}", repr(ret))

    def dataiku_sql_ctx(self):
        sc = pyspark.SparkContext.getOrCreate()
        return SQLContext(sc)
