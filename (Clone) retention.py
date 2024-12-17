# Databricks notebook source
import pandas as pd

# COMMAND ----------



q = '''
with main as
(select 
p.merchant_id,
md.business_dba,
DATE_FORMAT(cast(p.created_date as date) ,'yyyy-MM') AS created_month,
count(distinct p.id) as payment_attempts
 

from realtime_hudi_api.payments p
left join realtime_hudi_api.merchant_details md on p.merchant_id = md.merchant_id

where recurring = 0
and p.created_date >= '2022-01-01'
and p.created_date < '2024-12-01'


group by 1,2,3
 ) 

select main.merchant_id,
business_dba,
first_month,
created_month

from main
left join 
(select merchant_id, min(created_month) as first_month
from main 
 group by 1
) m2 on main.merchant_id = m2.merchant_id

where payment_attempts >= 10

'''

sprk_data=sqlContext.sql(q)
recurring_df =sprk_data.toPandas()
recurring_df.to_csv('/Workspace/Users/dronamraju.anurag@razorpay.com/non_recurring_df.csv', index=False)

