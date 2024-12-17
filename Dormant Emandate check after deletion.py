# Databricks notebook source
import pandas as pd


t1 ='''

With debit_data as (
  SELECT token_id,max(p.created_date) as max_debit_date 
  FROM realtime_hudi_api.payments p
  WHERE p.recurring=1
  and p.recurring_type='auto'
  and p.method in ('emandate','nach')
  and p.created_date>='2016-04-01'
  and p.created_at < '2024-10-01'

  GROUP BY 1
)

,term_data as (
  SELECT terminal_id,gateway,gateway_acquirer
  FROM realtime_terminalslive.terminals
  WHERE gateway in ('enach_npci_netbanking','enach_rbl','nach_citi','nach_icici')
  GROUP BY 1,2,3
)

,base as (SELECT distinct
    t.id as token_id
   ,SUBSTR(t.gateway_token,1,20) as UMRN_NO
   ,t.beneficiary_name
   ,t.created_date as MandateCreatedDate
   ,dd.max_debit_date as max_debit_date_date
   ,t.merchant_id as merchant_id
  ,md.business_dba
  ,t.frequency
  ,CASE WHEN dd.max_debit_date IS NOT NULL THEN DATE_DIFF(month, cast(dd.max_debit_date as date), CAST('2024-08-31' AS date))
  WHEN dd.max_debit_date IS NULL THEN DATE_DIFF(month, cast(t.created_date as date), CAST('2024-08-31' AS date))
  end as Dormancytenure_months 
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

  and t.method in ('emandate','nach')
  and term.gateway in ('enach_npci_netbanking','enach_rbl','nach_citi','nach_icici')
  and t.created_date < '2024-10-01'
  and t.created_date >= '2016-04-01'

)


  
SELECT 

  COALESCE(ftt.parent_name,base.business_dba) as parent_name
  ,ftt.managed_status
  ,ftt.team_owner
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
  END AS Penalty_Applicable,

  Count(distinct UMRN_NO) as UMRNs,
  Count(distinct token_id) as Tokens



  FROM base 
  LEFT JOIN aggregate_ba.final_team_tagging ftt
    ON ftt.merchant_id=base.merchant_id
  where Dormancytenure_months >= 6 
  and(case when Dormancytenure_months >= 6 AND Dormancytenure_months <= 12 and frequency = 'yearly' then 1 else 0 end) = 0


  GROUP BY 1,2,3,4,5

'''

# COMMAND ----------

sprk_data=sqlContext.sql(t1)
pd_df_tokens=sprk_data.toPandas()

# COMMAND ----------

pd_df_tokens

# COMMAND ----------

pd_df_tokens.to_csv('/Workspace/Users/dronamraju.anurag@razorpay.com/doramt_mandates_till_oct.csv')

# COMMAND ----------

len(pd_df_tokens)

# COMMAND ----------

# MAGIC %fs mkdirs /dbfs/FileStore/tables/Anurag/Dormant_Mandates/P0_2

# COMMAND ----------

import os
import pandas as pd

# Base path to save files in Databricks
base_path = "/dbfs/FileStore/tables/Anurag/Dormant_Mandates"

# Group data by parent_name
parent_groups = pd_df_tokens.groupby('parent_name')

# Iterate through each parent_name and split data into chunks of 500k rows
for parent_name, group_data in parent_groups:
    # Modify parent_name: replace spaces with underscores and remove " [M360]"
    parent_name_cleaned = parent_name.replace(" ", "_").replace(" [M360]", "")
    
    # Split data into chunks of 500,000 rows
    for i, chunk in enumerate(range(0, len(group_data), 500000)):
        # Get chunk of data
        data_chunk = group_data.iloc[chunk:chunk + 500000]
        
        # Define the CSV file name with an index after the parent_name
        if len(group_data) > 500000:
            file_name = f"{parent_name_cleaned}_part_{i + 1}.csv"
        else:
            file_name = f"{parent_name_cleaned}.csv"
        
        file_path = os.path.join(base_path, file_name)

        # Save the chunk to CSV
        data_chunk.to_csv(file_path, index=False)

        print(f"Saved {file_name} at {base_path}")


# COMMAND ----------

import os

# Replace with your folder path
folder_path = '/dbfs/FileStore/tables/Anurag/Dormant_Mandates/P0(2)'

# Check if the folder exists
if os.path.exists(folder_path):
    # List all files in the folder
    file_names = os.listdir(folder_path)

    # Print each file name
    for file_name in file_names:
        print(file_name)
else:
    print(f"Folder not found: {folder_path}")


# COMMAND ----------

df_1.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Bada_Business_Pvt_Ltd.csv')
df_2.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/HDB_Group.csv')
df_3.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Capital_Float_part_1.csv')
df_4.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Capital_Float_part_2.csv')
df_5.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Capital_Float_part_3.csv')
df_6.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Capital_Float_part_4.csv')
df_7.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Capital_Float_part_5.csv')
df_8.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Niyo.csv')
df_9.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Whizdm_part_1.csv')
df_10.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Whizdm_part_2.csv')
df_11.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Whizdm_part_3.csv')
df_12.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Navi_Finserv_Pvt_Ltd.csv')
df_13.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Eduvanz.csv')
df_14.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/EQX_Analytics_Pvt._Ltd..csv')
df_15.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/ClearTax.csv')
df_16.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/INDmoney_Group.csv')
df_17.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/ePaylater_Parent.csv')
df_18.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Aditya_Birla_Sun_Life_MF.csv')
df_19.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/GetSimpl.csv')
df_20.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Smartcoin.csv')
df_21.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Policybazaar.csv')
df_22.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/L&T_FINANCE.csv')
df_23.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Vivriti_Capital_Private_Limited_-_P.csv')
df_24.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Indiamart_Intermesh_Ltd.csv')
df_25.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Avanse_Group.csv')
df_26.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/ABFL.csv')
df_27.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/DMI_FINANCE_part_1.csv')
df_28.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/DMI_FINANCE_part_2.csv')
df_29.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/DMI_FINANCE_part_3.csv')
df_30.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/DMI_FINANCE_part_4.csv')
df_31.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/DMI_FINANCE_part_5.csv')
df_32.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/DRP_FINANCIAL_SERVICES_PVT_LTD_-_PA.csv')
df_33.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Sadguru_P_P_Moredada_Charitable_Hospital_&_Medical_Trust_-_PA.csv')
df_34.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/IPRU_Life_insurance.csv')
df_35.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/PIRAMAL_CAPITAL_AND_HOUSING_FINANCE_LIMITED.csv')
df_36.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/ET_Money_part_1.csv')
df_37.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/ET_Money_part_2.csv')
df_38.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Lendbox.csv')
df_39.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Zest_Money_PA_part_1.csv')
df_40.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Zest_Money_PA_part_2.csv')
df_41.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/HDFC_Life_Insurance_Parent_Account.csv')
df_42.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Bhanix_Finance.csv')
df_43.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Transerv_PA.csv')
df_44.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/WORTGAGE_FINANCE_PRIVATE_LIMITED_-_PA.csv')
df_45.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/MoneyTap.csv')
df_46.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Vivifi_india_finance_PVT_LTD_-_PA.csv')
df_47.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Scripbox.csv')
df_48.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Edelweiss_Lending.csv')
df_49.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Dreamplug_Technologies_[CRED]_Parent_part_1.csv')
df_50.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Dreamplug_Technologies_[CRED]_Parent_part_2.csv')
df_51.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Lendingkart.csv')
df_52.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Isha_Foundation_-_PA.csv')
df_53.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Social_Worth_Technologies_part_1.csv')
df_54.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Social_Worth_Technologies_part_2.csv')
df_55.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Axis_MF.csv')
df_56.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Rentomojo.csv')
df_57.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Motilal_Oswal_MF_-_Parent.csv')
df_58.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Kissht.csv')
df_59.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_Mandates/Creditt_(Datson_Group).csv')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from realtime_hudi_api.tokens where gateway_token = 'UBIN0000000009518244'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from realtime_hudi_api.tokens where SUBSTR(gateway_token,1,20) = 'UBIN0000000009518244'
