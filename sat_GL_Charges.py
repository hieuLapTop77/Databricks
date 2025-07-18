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
  	SELECT distinct
		lng_GL_Charges_Id_Nmbr,
		c.` dtm_GL_Charges_Date` dtm_GL_Charges_Date ,
		c.` str_GL_Charges_Desc` str_GL_Charges_Desc ,
		c.` mny_GL_Charges_Amount` mny_GL_Charges_Amount ,
		c.` mny_GL_Charges_Discount` mny_GL_Charges_Discount ,
		c.` mny_GL_Charges_Taxes` mny_GL_Charges_Taxes ,
		c.` mny_GL_Charges_Total` mny_GL_Charges_Total ,
		c.` str_GL_Charges_Notes` str_GL_Charges_Notes ,
		c.` str_Refundable_Charge` str_Refundable_Charge ,
		c.` str_Visible_Flag` str_Visible_Flag ,
		c.` mny_GL_Currency_Charges_Amount` mny_GL_Currency_Charges_Amount,
		c.` mny_Exchange_Rate` mny_Exchange_Rate ,
		c.` mny_GL_Currency_Charges_Discount` mny_GL_Currency_Charges_Discount ,
		c.` mny_GL_Currency_Charges_Taxes` mny_GL_Currency_Charges_Taxes ,
		c.` mny_GL_Currency_Charges_Total` mny_GL_Currency_Charges_Total,
		c.` dtm_Creation_Date` dtm_Creation_Date ,
		c.` dtm_Last_Mod_date` dtm_Last_Mod_date ,
		c.` str_Private_Flag` str_Private_Flag ,
		c.` bit_Fully_Paid` bit_Fully_Paid ,
		c.` str_Doc_Type_Flag` str_Doc_Type_Flag ,
		c.` str_Code` str_Code ,
		c.` str_Sub_Code` str_Sub_Code,
		CAST(c.` dtm_Last_Mod_date` AS DATE) AS  dtm_load_date ,
    	'dbamelia_vietjet_report.tbl_gl_charges' AS str_record_source 
    from pss_dinhnb.dbamelia_vietjet_report.tbl_gl_charges c
	where CAST(c.` dtm_Last_Mod_date` AS TIMESTAMP) > '{dtm_gl_charges}'
)
MERGE INTO pss_dinhnb.bronze.sat_gl_charges AS target
USING unioned AS source
ON target.lng_GL_Charges_Id_Nmbr = source.lng_GL_Charges_Id_Nmbr
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;
"""

# COMMAND ----------

spark.sql(query)