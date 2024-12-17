# Databricks notebook source
import pandas as pd


t1 ='''

With debit_data as (
SELECT p.token_id,max(p.created_date) as max_debit_date 
  FROM realtime_hudi_api.payments p
WHERE p.recurring=1
  and p.recurring_type='auto'
  and p.method in ('emandate','nach')
  and p.created_date>='2016-04-01'
GROUP BY 1
)

,term_data as (
SELECT terminal_id,gateway,gateway_acquirer
  FROM realtime_terminalslive.terminals
  WHERE gateway in ('enach_npci_netbanking','enach_rbl','nach_citi','nach_icici')
  GROUP BY 1,2,3
)

,base as (SELECT t.id as token_id
   ,SUBSTR(t.gateway_token,1,20) as UMRN_NO
   ,t.beneficiary_name
   ,t.created_date as MandateCreatedDate
   ,dd.max_debit_date as max_debit_date_date
   ,t.merchant_id as merchant_id
  ,md.business_dba
  ,CASE WHEN dd.max_debit_date IS NOT NULL THEN DATE_DIFF(month, cast(dd.max_debit_date as date), CAST('2024-08-31' AS date))
  WHEN dd.max_debit_date IS NULL THEN DATE_DIFF(month, cast(t.created_date as date), CAST('2024-08-31' AS date))
  end as Dormancytenure_months
  ,case when t.created_date > '2024-04-01' and t.frequency = 'yearly' then 1 else 0 end as token_not_eligible_flag 
  ,term.gateway_acquirer
  
FROM realtime_hudi_api.tokens t
LEFT JOIN debit_data dd
  ON dd.token_id=t.id
LEFT JOIN realtime_hudi_api.merchant_details md
  on md.merchant_id=t.merchant_id
INNER JOIN term_data term
    ON term.terminal_id=t.terminal_id
where t.recurring_status='confirmed'
and t.deleted_at is null
and (t.expired_at is null OR cast(from_unixtime(t.expired_at) as date)>CURRENT_DATE)
and t.method in ('emandate','nach')
and term.gateway in ('enach_npci_netbanking','enach_rbl','nach_citi','nach_icici')
and t.created_date < '2024-09-01'
and t.created_date > '2016-04-01'

)


  
SELECT 
  token_id
  ,UMRN_NO
  ,COALESCE(ftt.parent_name,base.business_dba) as parent_name
  ,ftt.managed_status
  ,ftt.team_owner
  ,base.MandateCreatedDate
  ,base.max_debit_date_date
  ,CASE WHEN Dormancytenure_months >= 6 AND Dormancytenure_months <= 12 THEN '6 Months - 1 Year'
  WHEN Dormancytenure_months > 12 AND Dormancytenure_months <= 24  THEN '1 - 2 Years'
  WHEN Dormancytenure_months > 24 AND Dormancytenure_months <= 36  THEN '2 - 3 Years'
  WHEN Dormancytenure_months > 36 AND Dormancytenure_months <= 48  THEN '3 - 4 Years'
  WHEN Dormancytenure_months > 48 AND Dormancytenure_months <= 60  THEN '4 - 5 Years'
  WHEN Dormancytenure_months > 48 AND Dormancytenure_months <= 60  THEN '5 - 6 Years'
  WHEN Dormancytenure_months > 60  THEN '6+ Years'
  END AS Dormancy_Bucket


  ,CASE WHEN Dormancytenure_months >= 6 AND Dormancytenure_months <= 12 THEN 1
  WHEN Dormancytenure_months > 12 AND Dormancytenure_months <= 24  THEN 2
  WHEN Dormancytenure_months > 24 AND Dormancytenure_months <= 36  THEN 2
  WHEN Dormancytenure_months > 36 AND Dormancytenure_months <= 48  THEN 3
  WHEN Dormancytenure_months > 48 AND Dormancytenure_months <= 60  THEN 3
  WHEN Dormancytenure_months > 48 AND Dormancytenure_months <= 60  THEN 5
  WHEN Dormancytenure_months > 60  THEN 5
  END AS Penalty_Applicable

FROM base 
LEFT JOIN aggregate_ba.final_team_tagging ftt
  ON ftt.merchant_id=base.merchant_id
where Dormancytenure_months >= 6 and token_not_eligible_flag = 0
and managed_status = 'Managed'

GROUP BY 1,2,3,4,5,6,7,8,9

'''

# COMMAND ----------

sprk_data=sqlContext.sql(t1)
pd_df_tokens=sprk_data.toPandas()

# COMMAND ----------

len(pd_df_tokens)

# COMMAND ----------

pd_df_tokens.to_csv("/dbfs/FileStore/tables/Rohit2819/master_file.csv")

# COMMAND ----------

import os
import pandas as pd

# Base path to save files in Databricks
base_path = "/Workspace/Users/dronamraju.anurag@razorpay.com/Dormant mandates managed 1"

# Group data by parent_name
parent_groups = pd_df_tokens.groupby('parent_name')

# Iterate through each parent_name and split data into chunks of 500k rows
for parent_name, group_data in parent_groups:
    # Split data into chunks of 500,000 rows
    for i, chunk in enumerate(range(0, len(group_data), 500000)):
        # Get chunk of data
        data_chunk = group_data.iloc[chunk:chunk + 500000]
        
        # Define the CSV file name with an index after the parent_name
        if len(group_data) > 500000:
            file_name = f"{parent_name}_part_{i + 1}.csv"
        else:
            file_name = f"{parent_name}.csv"
        
        file_path = os.path.join(base_path, file_name)

        # Save the chunk to CSV
        data_chunk.to_csv(file_path, index=False)

        print(f"Saved {file_name} at {base_path}")


# COMMAND ----------


