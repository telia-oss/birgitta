"""Utility function for cloning dataiku datasets including schema."""
from birgitta import context
from birgitta.dataiku.dataset import manage as dataset_manage


def clone(client,
          src_project_key,
          dst_project_key,
          src_name,
          dst_name,
          dst_dataset_type,
          copy_data=True):
    """Utility function for cloning dataiku datasets including schema.
    """
    src_project = client.get_project(src_project_key)
    dst_project = client.get_project(dst_project_key)
    dataset_manage.delete_if_exists(dst_project, dst_name)

    src_dataset = src_project.get_dataset(src_name)
    # src_dataset_definition = src_dataset.get_definition()
    if dst_dataset_type == "HDFS":
        # dst_dataset_type = src_dataset_definition['type']
        # dst_dataset_params = src_dataset_definition['params']
        dst_dataset_params = {
            'metastoreSynchronizationEnabled': True,
            'hiveDatabase': '${hive_table_work}',  # Use work
            'hiveTableName': '${projectKey}_' + dst_name,
            'connection': 'hdfs_work',
            'path': '/${projectKey}/' + dst_name,
            'notReadyIfEmpty': False
            #         'filesSelectionRules': {'mode': 'ALL',
            #             'excludeRules': [],
            #             'includeRules': [],
            #             'explicitFiles': []
            #         }
        }
        dst_dataset_params['importProjectKey'] = dst_project_key
        dst_format_type = "parquet"
        # dst_format_type = src_dataset_definition['formatType']
        #     dst_format_params = src_dataset_definition['formatParams']
        #     dst_format_params = {'parquetLowerCaseIdentifiers': False,
        #       'representsNullFields': False,
        #       'parquetCompressionMethod': 'SNAPPY',
        #       'parquetFlavor': 'HIVE',
        #       'parquetBlockSizeMB': 128}
    elif dst_dataset_type == "S3":
        s3_bucket = context.get('BIRGITTA_S3_BUCKET')
        dst_dataset_params = {
            'bucket': s3_bucket,
            'connection': 'S3',
            'path': '/${projectKey}/' + dst_name,
            'notReadyIfEmpty': False,
            #       'filesSelectionRules': {'mode': 'ALL',
            #        'excludeRules': [],
            #        'includeRules': [],
            #        'explicitFiles': []}
        }
        dst_format_type = "avro"
    else:  # Inline
        dst_dataset_params = {
            'keepTrackOfChanges': False,
            'notReadyIfEmpty': False,
            'importSourceType': 'NONE',
            'importProjectKey': dst_project_key
        }
        dst_format_type = "json"

    dst_dataset = dst_project.create_dataset(
        dst_name,
        dst_dataset_type,
        params=dst_dataset_params,
        formatType=dst_format_type
        # formatParams=dst_format_params # Prefer no hard typing
    )

    dst_dataset.set_schema(src_dataset.get_schema())
