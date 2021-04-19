# Databricks notebook source
import cldbx.utils.custom_logging as custom_logging
import cldbx.utils.structured_logging as structured_logging

processId = 1234

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Basic Log vs Structured Log

# COMMAND ----------

custom_logging.log_info("working")

structured_logging.log_info("working", [{"log_type": "GettingDataLogEvent"}])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Reading Data and Processed Data Events

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/flights/")

# COMMAND ----------

from datetime import datetime

today = datetime.now()

structured_logging.log_info("working", [{"log_type": "GettingData_LogEvent", "processId": processId, "TimeStarted": str(today.isoformat())}])

df = spark.read.format("csv").option("header", True).load("/databricks-datasets/flights/")

df.show()
structured_logging.log_info("working", [{"log_type": "GotData_LogEvent", "processId": processId, "RowCount": df.count(), "TimeStarted": str(today.isoformat())}])




# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Wrong Data Type Error

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/songs/data-001")

# COMMAND ----------

try:
  structured_logging.log_info("working", [{"log_type": "GettingData_LogEvent", "processId": processId, "TimeStarted": str(today.isoformat())}])
  df = spark.read.format("parquet").option("header", True).load("/databricks-datasets/flights/")
except:
  # Customise yuo capture iv
  structured_logging.log_error("working", [{"log_type": "WrongDataType_LogEvent", "processId": processId, "TimeStarted": str(today.isoformat())}])
  print("Wrong File Type")
  

# COMMAND ----------

# MAGIC %md
# MAGIC #Loads of Processes Starting 

# COMMAND ----------

for i in range(100):
  structured_logging.log_info("working", [{"log_type": "GettingData_LogEvent", "processId": processId, "TimeStarted": str(today.isoformat())}])
