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
and t.created_date > '2020-04-01'

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
where Dormancytenure_months >= 6 
and(case when Dormancytenure_months >= 6 AND Dormancytenure_months <= 12 and frequency = 'yearly' then 1 else 0 end) = 0
and managed_status = 'Managed'
and COALESCE(ftt.parent_name,base.business_dba) in
('Fatakpay Parent [M360]',
'CHINMAY FINLEASE LIMITED - PA [M360]',
'Credit Saison_PA [M360]',
'IIFL FINANCE LIMITED (Lending) [M360]',
'Greenpeace Environment Trust - PA [M360]',
'Northern Arc Capital Limited PA [M360]',
'RK BANSAL FINANCE - Parent [M360]',
'PayU Financial - PA [M360]',
'Multipl parent [M360]',
'Furlenco [M360]',
'CITYFURNISH INDIA PRIVATE LIMITED - PA [M360]',
'Apollo Finvest India Limited (Parent) [M360]',
'BMW Financials [M360]',
'FINNABLE CREDIT PRIVATE LIMITED - PA [M360]',
'Gajju Technologies Private Limited - PA [M360]',
'Aegon Life [M360]',
'Niva Bupa Parent [M360]',
'UNITY SMALL FINANCE BANK LIMITED [M360]',
'Edelweiss [M360]',
'OneCard App [M360]',
'Adityabirla Health insurance [M360]',
'Slice [M360]',
'JumboTail Parent Account [M360]',
'Techfino Capital Private Limited [M360]',
'Smallcase Technologies Private Limited [M360]',
'Bajaj Capital Limited [M360]',
'Ashv finance limited_PA [M360]',
'GRAYQUEST EDUCATION FINANCE PRIVATE LIMITED - PA [M360]',
'Rupeek Capital Pvt Ltd [M360]',
'RBL BANK LIMITED [M360]',
'No Broker (Parent Account) [M360]',
'MOTILAL HOME FINANCE GROUP [M360]',
'Motilal Group [M360]',
'ROINET [M360]',
'India Bulls Consumer Finance [M360]',
'AB Money Parent [M360]',
'ICICI LOMBARD [M360]',
'Principal Asset Management [M360]',
'BLACKSOIL CAPITAL PRIVATE LIMITED [M360]',
'UNI CARDS PARENT [M360]',
'Minions Ventures Pvt. Ltd. [M360]',
'SSRVM Parent [M360]',
'Liquiloans [M360]')

GROUP BY 1,2,3,4,5,6,7,8,9

'''

# COMMAND ----------

sprk_data=sqlContext.sql(t1)
pd_df_tokens=sprk_data.toPandas()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select gateway_token, id, ifsc
# MAGIC from realtime_hudi_api.tokens 
# MAGIC
# MAGIC where gateway_token in 
# MAGIC ('CITI0000000001676558',
# MAGIC 'CITI0000000001420954',
# MAGIC 'CITI0000000001420959',
# MAGIC 'CITI0000000001420958',
# MAGIC 'CITI0000000001420961',
# MAGIC 'CITI0000000001766748',
# MAGIC 'CITI7012211210002591',
# MAGIC 'CITI7012811210000059',
# MAGIC 'CITI7021012210000299',
# MAGIC 'CITI7021212210000086',
# MAGIC 'CITI7021712210001080',
# MAGIC 'CITI7011412210001089',
# MAGIC 'CITI7022412210000361',
# MAGIC 'CITI7011101220001866',
# MAGIC 'CITI7020601220001578',
# MAGIC 'CITI7021201220000787',
# MAGIC 'CITI7021002210000307',
# MAGIC 'CITI7020902210000126',
# MAGIC 'CITI7021702210000846',
# MAGIC 'CITI0000000001700330',
# MAGIC 'CITI7010703220001271',
# MAGIC 'CITI7010204220000487',
# MAGIC 'CITI7023103220001158',
# MAGIC 'CITI0000000001813378',
# MAGIC 'CITI7020406220000695',
# MAGIC 'CITI7011608220000782',
# MAGIC 'CITI7022507220000024',
# MAGIC 'CITI7010910220000058',
# MAGIC 'CITI7021712220001414',
# MAGIC 'CITI7010801230000483',
# MAGIC 'CITI7022002220000206',
# MAGIC 'CITI7021906230004006',
# MAGIC 'CITI7020507230002041',
# MAGIC 'CITI7010708230000105',
# MAGIC 'CITI7010604220003002',
# MAGIC 'CITI7012604220000198',
# MAGIC 'CITI7022704220000048',
# MAGIC 'CITI7010305220000656',
# MAGIC 'CITI7021806220000837',
# MAGIC 'CITI7021705220000143',
# MAGIC 'CITI7022701230000061',
# MAGIC 'CITI7012701230000782',
# MAGIC 'CITI7022612233000108',
# MAGIC 'CITI7020402243000035',
# MAGIC 'CITI7022512235000003',
# MAGIC 'CITI7021303245000166',
# MAGIC 'CITI7021104220000091',
# MAGIC 'CITI7021004243000063',
# MAGIC 'CITI7011703230001122',
# MAGIC 'CITI7022807220000791')
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM realtime_hudi_api.tokens LIMIT 10

# COMMAND ----------

t1 = '''

select * from realtime_hudi_api.tokens
limit 100
'''



# COMMAND ----------

sprk_data=sqlContext.sql(t1)
pd_df_tokens=sprk_data.toPandas()

# COMMAND ----------

pd_df_tokens.to_csv("/dbfs/FileStore/tables/Rohit2819/master_file.csv")

# COMMAND ----------

import os
import pandas as pd

# Base path to save files in Databricks
base_path = "/Workspace/Users/dronamraju.anurag@razorpay.com/P0 Managed Merchants"

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

pip install databricks-cli

# COMMAND ----------

# MAGIC %sh git --version
# MAGIC

# COMMAND ----------

import pandas as pd

# COMMAND ----------

d = pd.read_csv('/Workspace/Users/dronamraju.anurag@razorpay.com/P0 Managed Merchants/Capital Float [M360]_part_1.csv')

# COMMAND ----------

d.to_csv('/dbfs/FileStore/tables/Anurag/test.csv')

# COMMAND ----------

# MAGIC %fs mkdirs /dbfs/FileStore/tables/Anurag/Dormant_Mandates/P0_Managed_Merchants
# MAGIC

# COMMAND ----------

import os
from shutil import copytree

source_dir = '/Workspace/Users/dronamraju.anurag@razorpay.com/P0 Managed Merchants'

dest_dir = '/dbfs/FileStore/tables/Anurag/Dormant_Mandates'

# Use shutil to copy the entire directory
copytree(source_dir, dest_dir, dirs_exist_ok=True)


# COMMAND ----------

import shutil

# Define source and destination directories
workspace_source_dir = '/Workspace/Users/dronamraju.anurag@razorpay.com/P0 Managed Merchants'
dbfs_source_dir = '/dbfs/Workspace/Users/dronamraju.anurag@razorpay.com/P0 Managed Merchants'
dest_dir = '/dbfs/FileStore/tables/Anurag/Dormant_Mandates'

# Ensure destination directory exists
dbutils.fs.mkdirs(dest_dir)

# List files in the Workspace directory and copy them to DBFS
files = dbutils.fs.ls(workspace_source_dir)
for file_info in files:
    if file_info.isFile() and file_info.name.endswith('.csv'):
        # Copy from Workspace to DBFS
        dbutils.fs.cp(file_info.path, f'{dbfs_source_dir}/{file_info.name}')
        print(f'Copied {file_info.path} to {dbfs_source_dir}/{file_info.name}')


# COMMAND ----------

import pandas as pd

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


