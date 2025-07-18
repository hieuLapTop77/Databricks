# Databricks notebook source
import datetime
current_time_2h = datetime.datetime.now() - datetime.timedelta(hours=2)
datetime_gl_charges = current_time_2h.strftime('%Y-%m-%d %H:%M:%S')
dbutils.widgets.text("time_gl_charges", datetime_gl_charges, "Time Parameter")

# COMMAND ----------

dtm_gl_charges = dbutils.widgets.get("time_gl_charges")

# COMMAND ----------

query = f""" 
WITH unioned AS (
  SELECT
    distinct
    lng_GL_Charges_Id_Nmbr AS lng_GL_Charges_Id_Nmbr,
    c.` lng_Creation_User_Id_Nmbr` AS lng_User_Id_Nmbr,
    CAST(c.` dtm_Last_Mod_date` AS DATE) AS dtm_load_date,
    'dbamelia_vietjet_report.tbl_GL_Charges' AS str_record_source 
    from pss_dinhnb.dbamelia_vietjet_report.tbl_GL_Charges c
    where CAST(c.` dtm_Last_Mod_date` AS TIMESTAMP) > '{dtm_gl_charges}'
)
MERGE INTO pss_dinhnb.bronze.link_GL_Charges_Creation_User AS target
USING unioned AS source
ON target.lng_GL_Charges_Id_Nmbr = source.lng_GL_Charges_Id_Nmbr
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;
"""

# COMMAND ----------

spark.sql(query)