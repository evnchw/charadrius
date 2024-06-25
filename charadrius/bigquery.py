"""
Wrapper for BigQuery connection.

Resources
---------
https://www.rudderstack.com/guides/how-to-access-and-query-your-bigquery-data-using-python-and-r/
https://gcloud.readthedocs.io/en/latest/bigquery-client.html
"""

from aspidoceleon.constants import Constants
from google.cloud import bigquery
from google.cloud import bigquery_storage
from google.oauth2 import service_account
from google.protobuf import descriptor_pb2
from aspidoceleon.realtime.proto import market_trade_pb2
from google.cloud.bigquery_storage_v1.types import ProtoRows, ProtoSchema
import pandas as pd
import loguru

constants = Constants()


class BigQuery:
    def __init__(self, logger=None):
        if logger is None:
            self.logger = loguru.logger
        else:
            self.logger = logger
        self.project = constants.bq_project
        self.credentials = service_account.Credentials.from_service_account_file(constants.bq_creds_path)
        self.client = bigquery.Client(project=self.project, credentials=self.credentials)
        self.bq_budget_usd = constants.bq_budget_usd
        # Initialize
        self.datasets = sorted([i.dataset_id for i in self.client.list_datasets()])
        self.default_dataset = constants.bq_default_dataset
        assert self.default_dataset in self.datasets

    def tables(self, dataset: str = None, format="dict"):
        """
        Get list of tables (under all datasets) in BigQuery project.

        Parameters
        ----------
        dataset (str): dataset to get tables for, otherwise, get for all
        format (str): format for dataset-table, in ('str','dict')

        Returns
        -------
        generator of dict
        """
        assert format in ("str", "dict")
        datasets = self.datasets
        if dataset is None:
            datasets = [self.default_dataset]
        for dataset in datasets:
            dataset_ref = self.client.dataset(dataset)
            tables = self.client.list_tables(dataset_ref)  # table object
            for table in tables:
                if format == "str":
                    yield f"{dataset}.{table.table_id}"
                elif format == "dict":
                    yield {"dataset": dataset, "table": table.table_id, "table_ref": table}
                else:
                    raise Exception("Not supported.")

    def estimate_query_cost(self, sql: str, cost_per_tb_usd: float = 5.0):
        """
        Estimate query cost.

        Parameters
        ----------
        sql (str): query to estimate cost for
        cost_per_tb_usd (float): cost per terabyte, in USD

        Returns
        -------
        float
        """
        # Configure the query job to perform a dry run, and obtain bytes
        # that would be processed
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        query_job = self.client.query(sql, job_config=job_config)
        estimated_bytes = query_job.total_bytes_processed
        # BigQuery pricing: $5 per TB (1 TB = 2^40 bytes)
        bytes_in_tb = 2**40
        estimated_cost = (estimated_bytes / bytes_in_tb) * cost_per_tb_usd
        estimated_gb = estimated_bytes / (10**9)
        self.logger.info(f"Estimated query cost: ${estimated_cost:.2f} ({estimated_gb:.1f} GB)")
        return estimated_cost

    def query(self, sql):
        """
        Run a BigQuery SQL query.
        Simple runner: just submit and wait for it to execute, with some failure tolerance.

        Parameters
        ----------
        sql (str): the string query to run

        Returns
        -------
        pd.DataFrame
        """
        query_cost = self.estimate_query_cost(sql)
        if query_cost >= self.bq_budget_usd["warning"]:
            self.logger.warning(f"Exceeds budget (warning): {query_cost} >= {self.bq_budget_usd['warning']}.")
        if query_cost >= self.bq_budget_usd["error"]:
            self.logger.error(f"Exceeds budget (error): {query_cost} >= {self.bq_budget_usd['error']}.")
            return

        try:
            return self.client.query_and_wait(sql).to_dataframe()
        except Exception as e:
            self.logger.error("Ran into an error while querying.")
            self.logger.error(e)

    def upload(self, df: pd.DataFrame, dataset: str, table: str, write_disposition: str = "replace"):
        """
        Upload a Pandas dataframe to [dataset].[table].

        Parameters
        ----------
        df (pd.DataFrame): the dataframe to upload
        dataset (str): the dataset name (under the BigQuery project)
        table (str): the table
        write_disposition (str): how to write the table

        Returns
        -------
        BigQuery job result
        """

        assert write_disposition in ("replace", "append", "empty")
        self.logger.info("preparing to upload")
        dispositions = {
            "replace": bigquery.WriteDisposition.WRITE_TRUNCATE,
            "append": bigquery.WriteDisposition.WRITE_APPEND,
            "empty": bigquery.WriteDisposition.WRITE_EMPTY,
        }
        job_config = bigquery.LoadJobConfig(
            write_disposition=dispositions[write_disposition],
            source_format=bigquery.SourceFormat.PARQUET,  # Use Parquet format for efficiency
        )
        table_ref = self.client.dataset(dataset).table(table)

        # Upload the Pandas DataFrame to BigQuery
        self.logger.info("uploading to BigQuery")
        try:
            res = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
            return res
        except Exception as e:
            self.logger.error("Ran into an error while uploading.")
            self.logger.error(e)

    def stream_insert(self, proto_rows: ProtoRows, schema_descriptor, dataset: str, table: str):
        """Insert items via BigQuery Streaming API."""
        # -------------------------------------
        # Create protocol buffers, following this example:
        # https://stackoverflow.com/questions/76850444/bigquery-storage-api-what-happened-to-appendrowsstream
        # https://cloud.google.com/bigquery/docs/write-api-batch
        # https://cloud.google.com/blog/products/bigquery/life-of-a-bigquery-streaming-insert

        # Create write client
        credentials = service_account.Credentials.from_service_account_file(constants.bq_creds_path)
        client = bigquery_storage.BigQueryWriteClient(credentials=credentials)

        # Create a write stream request
        # Seems to take a little time to build the table, otherise:
        # future: <Task finished name='Task-13' coro=<WSBase._message_handler() done, defined at
        # /root/jupyterlab/aspidoceleon/.venv/lib/python3.10/site-packages/coinbase/websocket/websocket_base.py:518>
        # exception=NotFound('Requested entity was not found.
        # Entity: projects/fastitocalon/datasets/coinbase/tables/rt_coinbase_ticker_batch_btcusd_20240528/
        #     streams/Cic3YjVhY2RiZi0wMDAwLTJiOWQtYTAwZS0xNDIyM2JiOWVhYzY6czI')>
        request = bigquery_storage.CreateWriteStreamRequest(
            # parent path is different for CreateWriteStreamRequest
            parent=f"projects/{constants.bq_project}/datasets/{dataset}/tables/{table}",
            write_stream=bigquery_storage.WriteStream(type_=bigquery_storage.WriteStream.Type.PENDING),
        )
        stream = client.create_write_stream(request=request)

        # Initialize data object for rows
        # - add data schema (which columns etc.), copied from proto .py file
        # - add the rows (the actual data)
        proto_data = bigquery_storage.AppendRowsRequest.ProtoData()
        proto_schema = bigquery_storage.ProtoSchema()
        proto_descriptor = descriptor_pb2.DescriptorProto()
        schema_descriptor.CopyToProto(proto_descriptor)
        proto_schema.proto_descriptor = proto_descriptor
        proto_data.writer_schema = proto_schema
        proto_data.rows = proto_rows
        append_rows_iter = (
            bigquery_storage.AppendRowsRequest(write_stream=stream.name, proto_rows=item) for item in [proto_data]
        )
        client.append_rows(requests=append_rows_iter)

        # Finalize the stream.
        client.finalize_write_stream(name=stream.name)

        # Batch commit the stream.
        batch_commit_request = bigquery_storage.BatchCommitWriteStreamsRequest(
            parent=f"projects/{constants.bq_project}/datasets/{dataset}/tables/{table}",
            write_streams=[stream.name],
        )
        commit_response = client.batch_commit_write_streams(request=batch_commit_request)

    def stream_insert_legacy(self, rows: list, dataset: str, table: str):
        """
        BigQuery streaming insert a row into [dataset.table].

        Parameters
        ----------
        rows (list of dict): list of dictionaries (typed values) to insert into [dataset.table]
            Assumes column schema match: see create_table().
        dataset (str): dataset to insert into
        table (str): dataset to insert into
        """
        table_ref = self.client.dataset(dataset).table(table)
        errors = self.client.insert_rows_json(table_ref, rows)
        try:
            # success
            assert errors == []
        except:
            # fail
            errors_str = "\n".join([str(i) for i in errors])
            raise Exception(f"Errors encountered while inserting rows: {errors_str}")

    def table_exists(self, dataset: str, table: str):
        """
        Check if BigQuery table [dataset.table] exists.
        """
        table_ref = self.client.dataset(dataset).table(table)
        try:
            self.client.get_table(table_ref)
            return True
        except Exception as e:
            if "Not found" in str(e):
                return False
            else:
                self.logger.error("Error while checking if table exists.")
                raise e

    def _pandas_to_bq_schema(self, df: pd.DataFrame) -> list:
        """
        Helper to convert pandas dataframe
        """
        type_mapping = {
            "object": "STRING",
            "int64": "INTEGER",
            "float64": "FLOAT",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP",
            "datetime64[ns, UTC]": "TIMESTAMP",
            "datetime64[us]": "TIMESTAMP",
            "datetime64[us, UTC]": "TIMESTAMP",
            # possibly other stuff that is not supported here yet
        }

        schema = []
        for column in df.columns:
            dtype = str(df[column].dtype)
            if dtype in type_mapping:
                schema.append(bigquery.SchemaField(column, type_mapping[dtype]))
            else:
                raise ValueError(f"Unsupported dtype {dtype} for column {column}")
        return schema

    def create_table(self, dataset: str, table: str, df: pd.DataFrame = None, dtypes: list = None) -> str:
        """
        Create [dataset.table] if it does not exist.

        Parameters
        ----------
        dataset (str): name of dataset
        table (str): name of table
        df (pd.DataFrame): if provided, auto-detect column types from here
        dtypes (list of google.cloud.bigquery.SchemaField): alternatively, typed columns to create

        One of (df, dtypes) must not be None.

        Returns
        -------
        str: the final [dataset.table]
        """
        if df is not None:
            schema_cols = self._pandas_to_bq_schema(df)
        elif dtypes is not None:
            schema_cols = dtypes
        else:
            raise Exception("Must provide one of ('df', 'dtypes').")
        table_ref = self.client.dataset(dataset).table(table)
        tbl = bigquery.Table(table_ref, schema=schema_cols)
        tbl = self.client.create_table(tbl)
        print(f"Table created: [{dataset}.{table}]")
        return f"""{dataset}.{table}"""

    def set_retention(self, dataset: str, table: str, retention_date: pd.Timestamp, overwrite: bool = False):
        """
        Set retention for a table.

        Parameters
        ----------
        dataset (str): BQ dataset name
        table (str): BQ table name
        retention_date (pd.Timestamp): expiry date, if no timezone will be UTC. If None, unsets retention.
        overwrite (bool):
            If True, overwrites any retention (incl. None).
            If False, only sets retention IF no retention (=None). Good to use on the first time creating table.

        Returns
        -------
        None
        """

        self.logger.info(f"Setting retention for {dataset}.{table}: {retention_date}.")
        table_ref = self.client.dataset(dataset).table(table)
        table_obj = self.client.get_table(table_ref)
        if overwrite is False and table_obj.expires is not None:
            self.logger.warning(f"Retention exists already: {table_obj.expires}. Exiting ...")
            return
        table_obj.expires = retention_date
        self.client.update_table(table_obj, ["expires"])
        if retention_date is not None:
            self.logger.info(f"Updated retention ==> {retention_date}.")
        else:
            self.logger.info(f"Removed retention.")


if __name__ == "__main__":
    bq = BigQuery()
