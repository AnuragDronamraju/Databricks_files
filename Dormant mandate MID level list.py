# Databricks notebook source
import pandas as pd
import numpy as np

citi = pd.read_csv('/Workspace/Users/dronamraju.anurag@razorpay.com/ack_citi_bank_file.csv')
yesb = pd.read_csv('/Workspace/Users/dronamraju.anurag@razorpay.com/YES-BANK-ACK-FILES.csv')


# COMMAND ----------

c_mandate_ids = citi.loc[citi['Accepted'] == np.True_, 'OriginalMandateID'].tolist()
c_mandate_ids_rejected = citi.loc[citi['Accepted'] == np.False_, 'OriginalMandateID'].tolist()

y_mandate_ids = yesb.loc[yesb['Accepted'] == np.True_, 'OriginalMandateID'].tolist()
y_mandate_ids__rejected = yesb.loc[yesb['Accepted'] == np.False_, 'OriginalMandateID'].tolist()

print("Total citi bank mandates: ",len(citi))
print("Total citi bank mandates cancelled: ",len(c_mandate_ids))
print("Total citi bank mandates rejected: ",len(c_mandate_ids_rejected))

print("Total yesb bank mandates: ",len(yesb))
print("Total yesb bank mandates cancelled: ",len(y_mandate_ids))
print("Total yesb bank mandates rejected: ",len(y_mandate_ids__rejected))


# COMMAND ----------


q = '''
select t.merchant_id, t.gateway_token,COALESCE(ftt.parent_name,md.business_dba) as parent_name, md.business_dba, t.created_at
from realtime_hudi_api.tokens t
left join realtime_hudi_api.merchant_details md on t.merchant_id = md.merchant_id
LEFT JOIN aggregate_ba.final_team_tagging ftt on ftt.merchant_id=t.merchant_id

where t.gateway_token in ({})
'''.format(c_mandate_ids)
q=q.replace('])',')')
q=q.replace('([','(')
sprk_data=sqlContext.sql(q)
citi_mapped=sprk_data.toPandas()
citi_mapped.to_csv('/Workspace/Users/dronamraju.anurag@razorpay.com/citibank_cancelled_mapped.csv', index=False)

# COMMAND ----------

citi_mapped

# COMMAND ----------

# citi_cleaned = citi_mapped.groupby('gateway_token')['created_at'].max().reset_index()

# citi_final_mapped = citi_mapped.merge(citi_cleaned, on=['gateway_token', 'created_at'], how='inner')

# citi_final_mapped = citi_final_mapped.drop_duplicates()

# print(len(citi_final_mapped))

# citi_final_mapped.to_csv('/Workspace/Users/dronamraju.anurag@razorpay.com/citibank_cancelled_mapped_final.csv', index=False)

citi_final_mapped = pd.read_csv('/Workspace/Users/dronamraju.anurag@razorpay.com/citibank_cancelled_mapped_final.csv')

distinct_count_df = citi_final_mapped.groupby(['parent_name', 'business_dba', 'merchant_id']).agg({'gateway_token': 'nunique'}).reset_index()

# Rename the columns for better readability
distinct_count_df.columns = ['parent_name','business_dba','merchant_id', 'distinct_gateway_token_count']

# Write the DataFrame to a CSV file
distinct_count_df.to_csv('/Workspace/Users/dronamraju.anurag@razorpay.com/citibank_cancelled_mapped_summary.csv', index=False)

print("CSV file 'citibank_cancelled_mapped_summary_2.csv' has been created.")

# COMMAND ----------

distinct_count_df

# COMMAND ----------


q = '''
select t.merchant_id, t.gateway_token,COALESCE(ftt.parent_name,md.business_dba) as parent_name, md.business_dba, t.created_at
from realtime_hudi_api.tokens t
left join realtime_hudi_api.merchant_details md on t.merchant_id = md.merchant_id
LEFT JOIN aggregate_ba.final_team_tagging ftt on ftt.merchant_id=t.merchant_id

where t.gateway_token in ({})
'''.format(y_mandate_ids)
q=q.replace('])',')')
q=q.replace('([','(')
sprk_data=sqlContext.sql(q)
yesb_mapped=sprk_data.toPandas()
yesb_mapped.to_csv('/Workspace/Users/dronamraju.anurag@razorpay.com/yesbank_cancelled_mapped.csv', index=False)

# COMMAND ----------

# yesb_cleaned = yesb_mapped.groupby('gateway_token')['created_at'].max().reset_index()

# yesb_final_mapped = yesb_mapped.merge(yesb_cleaned, on=['gateway_token', 'created_at'], how='inner')

# yesb_final_mapped = yesb_final_mapped.drop_duplicates()

# print(len(yesb_final_mapped))

# yesb_final_mapped.to_csv('/Workspace/Users/dronamraju.anurag@razorpay.com/yesbank_cancelled_mapped_final.csv', index=False)

yesb_final_mapped = pd.read_csv('/Workspace/Users/dronamraju.anurag@razorpay.com/yesbank_cancelled_mapped_final.csv')

distinct_count_df_2 = yesb_final_mapped.groupby(['parent_name', 'business_dba', 'merchant_id']).agg({'gateway_token': 'nunique'}).reset_index()

# Rename the columns for better readability
distinct_count_df_2.columns = ['parent_name','business_dba','merchant_id', 'distinct_gateway_token_count']

# Write the DataFrame to a CSV file
distinct_count_df_2.to_csv('/Workspace/Users/dronamraju.anurag@razorpay.com/yesbank_cancelled_mapped_summary_2.csv', index=False)

print("CSV file 'yesbank_cancelled_mapped_summary_2.csv' has been created.")

# COMMAND ----------


