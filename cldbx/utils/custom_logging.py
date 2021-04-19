from cldbx.utils.base_logging import log_critical as base_log_critical, log_error as base_log_error, log_info as base_log_info, log_warning as base_log_warning
from pyspark.sql import SparkSession

global wks_id
global wks_shared_key

spark = SparkSession.builder.getOrCreate()


def get_dbutils(spark):
    if spark.conf.get("spark.databricks.service.client.enabled") == "true":
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    else:
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]


dbutils = get_dbutils(spark)


# Retrieve your Log Analytics Workspace ID from your Key Vault Databricks Secret Scope
wks_id = dbutils.secrets.get(scope="KVEngineering", key="LogAnalyticsWSID")
# Retrieve your Log Analytics Primary Key from your Key Vault Databricks Secret Scope
wks_shared_key = dbutils.secrets.get(scope="KVEngineering", key="LogAnalyticsWsKey")


def log_info(message):
    dbx_body = [{
        "Application": dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
        "ClusterId": dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('clusterId'),
        "SessionId": dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('sessionId'),
        "Who": dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user'),
        "Message": message}]
    base_log_info(wks_id, wks_shared_key, dbx_body)


def log_error(message):
    dbx_body = [{
        "Application": dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
        "ClusterId": dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('clusterId'),
        "SessionId": dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('sessionId'),
        "Who": dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user'),
        "Message": message}]
    base_log_error(wks_id, wks_shared_key, dbx_body)


def log_warning(message):
    dbx_body = [{
        "Application": dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
        "ClusterId": dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('clusterId'),
        "SessionId": dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('sessionId'),
        "Who": dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user'),
        "Message": message}]
    base_log_warning(wks_id, wks_shared_key, dbx_body)


def log_critical(message):
    dbx_body = [{
        "Application": dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
        "ClusterId": dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('clusterId'),
        "SessionId": dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('sessionId'),
        "Who": dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user'),
        "Message": message}]
    base_log_critical(wks_id, wks_shared_key, dbx_body)
