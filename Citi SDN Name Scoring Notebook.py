# Databricks notebook source
!pip install thefuzz

# COMMAND ----------

#importing libraries
from thefuzz import fuzz
from thefuzz import process 
import time
import pandas as pd
import numpy as np

# COMMAND ----------

#names_to_be_flagged=pd.read_csv ('citi_sdn_names_to_be_updated_may24.csv')
data = {'names_to_be_flagged': ['Sandeep Kumar', 'Mahendra Singh']}

# Create DataFrame
names_to_be_flagged = pd.DataFrame(data)

# COMMAND ----------

query='''WITH terminal_data as (
SELECT terminal_id,gateway,gateway_acquirer,CASE WHEN merchant_id in ('100DemoAccount','100000Razorpay') THEN 'Shared' ELSE 'Direct' END as shared_direct FROM realtime_terminalslive.terminals
WHERE deleted_at is null
  GROUP BY 1,2,3,4
)

,payment_data as(
SELECT token_id
FROM realtime_hudi_api.payments
  WHERE created_date>='2023-05-01'
  AND method in ('emandate','nach')
  GROUP BY 1
)

SELECT CASE WHEN pd.token_id is not null then 1 else 0 end as activve_l1y
  ,t.id as token_id
  ,t.beneficiary_name
  ,td.shared_direct
  ,t.method
FROM realtime_hudi_api.tokens t
LEFT JOIN terminal_data td
  ON td.terminal_id=t.terminal_id
LEFT JOIN payment_data pd
  ON pd.token_id=t.id
WHERE t.method in ('emandate','nach')
AND t.recurring_status='confirmed'
and t.auth_type not in ('aadhaar', 'migrated')
AND t.deleted_at is NULL
AND (t.expired_at is null OR from_unixtime(t.expired_at)>CURRENT_TIMESTAMP)
AND (td.gateway='nach_citi' or td.gateway_acquirer='citi')
GROUP BY 1,2,3,4,5'''

# COMMAND ----------

sprk_data=sqlContext.sql(query)
emandate_tokens=sprk_data.toPandas()

# COMMAND ----------

emandate_tokens['beneficiary_name'] = emandate_tokens['beneficiary_name'].astype(str)

# COMMAND ----------

emandate_tokens["token_set_fuzzy_score"]=0

# COMMAND ----------

emandate_tokens.head()

# COMMAND ----------

len(emandate_tokens)

# COMMAND ----------

# token_set_ratio

emandate_tokens["token_set_fuzzy_match"] = emandate_tokens["beneficiary_name"].apply(
    lambda x: process.extractOne(x.lower(), names_to_be_flagged["names_to_be_flagged"].str.lower(), scorer=fuzz.token_set_ratio)[0]
)

for i in range(0,len(emandate_tokens)):
    emandate_tokens["token_set_fuzzy_score"][i]= fuzz.token_set_ratio(emandate_tokens["beneficiary_name"][i].lower(),emandate_tokens["token_set_fuzzy_match"][i].lower())

# COMMAND ----------

emandate_tokens['sdn_rzp_flag'] = [1 if x >=75 else 0 for x in emandate_tokens['token_set_fuzzy_score']]
emandate_tokens_filtered = emandate_tokens[emandate_tokens['sdn_rzp_flag'] ==1]

# COMMAND ----------

emandate_tokens_filtered.to_csv('/dbfs/FileStore/tables/Anurag/emandate_citi_tokens_scored_filtered_dec10.csv')

# COMMAND ----------

import pandas as pd

# COMMAND ----------

pip install xlsxwriter

# COMMAND ----------

pip install openpyxl

# COMMAND ----------

#Importing libraries
import pandas as pd
import numpy as np
import io
import smtplib
import datetime
import openpyxl
import csv
import math
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime as dt

# COMMAND ----------

#mailing metrics
sender_address='dronamraju.anurag@razorpay.com'
mail_token='iprx ytom bcux ogdw'
receiver_address=['dronamraju.anurag@razorpay.com']

# COMMAND ----------

emandate_tokens=pd.read_csv('/dbfs/FileStore/tables/Anurag/emandate_citi_tokens_scored_filtered_dec10.csv')
emandate_tokens['sdn_rzp_flag'] = [1 if x >=75 else 0 for x in emandate_tokens['token_set_fuzzy_score']]
emandate_tokens = emandate_tokens[emandate_tokens['sdn_rzp_flag']==1]

token_list = emandate_tokens['token_id'].tolist()

# COMMAND ----------

len(token_list)

# COMMAND ----------

def export_excel_yesb_format(df1):
  with io.BytesIO() as buffer:
    with pd.ExcelWriter(buffer, engine='xlsxwriter',datetime_format="mm/dd/yyyy hh:mm:ss AM/PM") as writer:
        df1.to_excel(writer,sheet_name="Sheet1",index=False,startrow=0, startcol=0)
        workbook=writer.book
        worksheet1 = writer.sheets["Sheet1"]
        format = workbook.add_format({'text_wrap': False})
        format1= workbook.add_format({'num_format': '@'})
        format2= workbook.add_format({'num_format': 'mm/dd/yyyy hh:mm:ss AM/PM'})
        # Setting the format but not setting the column width.
        worksheet1.set_column('D:D', 10, format1)
        worksheet1.set_column('R:R', 10, format2)
        worksheet1.set_column('AA:AA', 10, format2)
        worksheet1.set_column('AC:AC', 10, format2)
        writer.close()
    return buffer.getvalue()

# COMMAND ----------

def export_excel_token_list(df1):
  with io.BytesIO() as buffer:
    with pd.ExcelWriter(buffer, engine='xlsxwriter',datetime_format="mm/dd/yyyy hh:mm:ss AM/PM") as writer:
        df1.to_excel(writer,sheet_name="Sheet1",index=False,startrow=0, startcol=0)
        workbook=writer.book
        worksheet1 = writer.sheets["Sheet1"]
        format = workbook.add_format({'text_wrap': False})
        format1= workbook.add_format({'num_format': '@'})
        format2= workbook.add_format({'num_format': 'mm/dd/yyyy hh:mm:ss AM/PM'})
        # Setting the format but not setting the column width.
        worksheet1.set_column('A:L', 10, format1)
        writer.close()
    return buffer.getvalue()

# COMMAND ----------

def mailer_w_attach(body,subject,sender_pass,sender_add,receiver_add,EXPORTERS,DB_List):
    #Setup the MIME
    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = ", ".join(receiver_address)
    #message['CC'] = ", ".join(cc_address)
    today = datetime.date.today()
    message['Subject'] = subject   #The subject line
    part = MIMEText(body, "html")
    for filename in EXPORTERS:
        attachment = MIMEApplication(EXPORTERS[filename](DB_List[filename]))
        attachment['Content-Disposition'] = 'attachment; filename="{}"'.format(filename)
        message.attach(attachment)
    message.attach(part)
    #Create SMTP session for sending the mail
    session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
    session.starttls() #enable security
    session.login(sender_address, sender_pass) #login with mail_id and password
    text = message.as_string()
    session.sendmail(sender_address, receiver_address, text)
    session.quit()

# COMMAND ----------

#V2 Shared token_ID input

count_of_run=math.ceil(len(token_list)/50000)
for i in range(count_of_run):
    

    #All token list
    query='''
    WITH terminal_data as (
    SELECT terminal_id,gateway,gateway_acquirer,merchant_id FROM realtime_terminalslive.terminals
    GROUP BY 1,2,3,4
    )
    SELECT tok.id as token_id
    ,tok.gateway_token
    ,tok.merchant_id
    ,md.business_dba
    ,tok.method
    ,term.gateway
    ,term.gateway_acquirer
    ,tok.terminal_id
    ,CASE WHEN term.merchant_id in ('100DemoAccount','100000Razorpay') THEN 'Shared' ELSE 'Direct' END as shared_direct
    ,tok.beneficiary_name
    ,ftt.name
    ,ftt.team_owner
    FROM realtime_hudi_api.tokens tok
    LEFT JOIN realtime_hudi_api.merchant_details md
        ON md.merchant_id=tok.merchant_id
    LEFT JOIN terminal_data term
        ON term.terminal_id=tok.terminal_id
    LEFT JOIN aggregate_ba.final_team_tagging ftt
        ON ftt.merchant_id=tok.merchant_id
    WHERE tok.id in ({})
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
    '''.format(token_list)
    query=query.replace('])',')')
    query=query.replace('([','(')
    sprk_data=sqlContext.sql(query)
    pd_df_tokens=sprk_data.toPandas() 

    #Yes Bank Format Data
    list_of_tokens=pd_df_tokens[(pd_df_tokens['method']=='emandate')&
                                (pd_df_tokens['shared_direct']=='Shared') & 
                                (pd_df_tokens['gateway_acquirer']=='citi')].token_id.to_list()

    query='''
    WITH terminal_data as (
    SELECT terminal_id,gateway,gateway_acquirer,get_json_object(identifiers,'$.gateway_merchant_id2') as gateway_merchant_id2 FROM realtime_terminalslive.terminals
    GROUP BY 1,2,3,4
    )

    ,min_debit_date as (SELECT p.token_id
    ,min((p.created_at+19800)) as MANDATE_ACCEPTANCE_DATE
    FROM realtime_hudi_api.payments p
    where p.created_date>='2019-01-01'
    and p.recurring_type='auto'
    and p.token_id in ({})
    GROUP BY 1)

    SELECT SUBSTR(tok.gateway_token,1,20) as UMRN_NO
        ,'ACTIVE' as SYSTEM_STATUS
        ,'DEBIT' as PAYMENTTYPE
        ,SUBSTR(cast(tok.account_number as char(40)),1,35) as DEBTORACCOUNTNO
        ,SUBSTR(CASE WHEN tok.bank='HDFC' THEN 'HDFC BANK LTD'
            WHEN tok.bank='ICIC' THEN 'ICICI BANK LTD'
            WHEN tok.bank='SBIN' THEN 'STATE BANK OF INDIA'
            WHEN tok.bank='KKBK' THEN 'KOTAK MAHINDRA BANK LTD'
            WHEN tok.bank='YESB' THEN 'YES BANK'
            WHEN tok.bank='CNRB' THEN 'CANARA BANK'
            WHEN tok.bank='INDB' THEN 'INDUSIND BANK'
            WHEN tok.bank='BARB' THEN 'BANK OF BARODA'
            WHEN tok.bank='SCBL' THEN 'STANDARD CHARTERED BANK'
            WHEN tok.bank='PYTM' THEN 'PAYTM PAYMENTS BANK LTD'
            WHEN tok.bank='UTIB' THEN 'AXIS BANK'
            WHEN tok.bank='CBIN' THEN 'CENTRAL BANK OF INDIA'
            WHEN tok.bank='DBSS' THEN 'DBS BANK INDIA LTD'
            WHEN tok.bank='PUNB' THEN 'PUNJAB NATIONAL BANK'
            WHEN tok.bank='ESFB' THEN 'EQUITAS SMALL FINANCE BANK LTD'
            WHEN tok.bank='IBKL' THEN 'IDBI BANK'
            WHEN tok.bank='CIUB' THEN 'CITY UNION BANK LTD'
            WHEN tok.bank='IDFB' THEN 'IDFC FIRST BANK LTD'
            WHEN tok.bank='CITI' THEN 'CITIBANK N A'
            WHEN tok.bank='UBIN' THEN 'UNION BANK OF INDIA'
            WHEN tok.bank='BKID' THEN 'BANK OF INDIA'
            WHEN tok.bank='FDRL' THEN 'FEDERAL BANK'
            WHEN tok.bank='IOBA' THEN 'INDIAN OVERSEAS BANK'
            WHEN tok.bank='RATN' THEN 'RBL BANK LIMITED'
            WHEN tok.bank='AUBL' THEN 'AU SMALL FINANCE BANK'
            WHEN tok.bank='MAHB' THEN 'BANK OF MAHARASHTRA'
            WHEN tok.bank='DCBL' THEN 'DCB BANK LTD'
            WHEN tok.bank='KARB' THEN 'KARNATAKA BANK LTD'
            WHEN tok.bank='IDIB' THEN 'INDIAN BANK'
            WHEN tok.bank='BDBL' THEN 'BANDHAN BANK LTD'
            WHEN tok.bank='PUNB_R' THEN 'PUNJAB NATIONAL BANK'
            WHEN tok.bank='BARB_R' THEN 'BANK OF BARODA'
            WHEN tok.bank='UTKS' THEN 'UTKARSH SMALL FINANCE BANK'
            WHEN tok.bank='HSBC' THEN 'HSBC BANK'
            WHEN tok.bank='ANDB' THEN 'ANDHRA BANK'
            WHEN tok.bank='UJVN' THEN 'UJJIVAN SMALL FINANCE BANK LTD'
            WHEN tok.bank='AIRP' THEN 'AIRTEL PAYMENTS BANK LTD'
            WHEN tok.bank='SIBL' THEN 'THE SOUTH INDIAN BANK LIMITED'
            WHEN tok.bank='KVBL' THEN 'KARUR VYSA BANK'
            WHEN tok.bank='TMBL' THEN 'TAMILNAD MERCANTILE BANK LTD'
            WHEN tok.bank='UTBI' THEN 'UNITED BANK OF INDIA'
            WHEN tok.bank='USFB' THEN 'UJJIVAN SMALL FINANCE BANK LTD'
            WHEN tok.bank='JSFB' THEN 'JANA SMALL FINANCE BANK LTD'
            WHEN tok.bank='DLXB' THEN 'DHANALAXMI BANK'
            WHEN tok.bank='NSPB' THEN 'NSDL Payments Banks Ltd'
            WHEN tok.bank='UCBA' THEN 'UCO BANK'
            WHEN tok.bank='FINF' THEN 'AU SMALL FINANCE BANK'
            WHEN tok.bank='APGB' THEN 'ANDHRA PRAGATHI GRAMEENA BANK'
            WHEN tok.bank='COSB' THEN 'THE COSMOS CO-OPERATIVE BANK LTD'
            WHEN tok.bank='JAKA' THEN 'THE JAMMU AND KASHMIR BANK LTD'
            WHEN tok.bank='PSIB' THEN 'PUNJAB AND SIND BANK'
            WHEN tok.bank='SURY' THEN 'SURYODAY SMALL FINANCE BANK LTD'
            WHEN tok.bank='DEUT' THEN 'DEUTSCHE BANK AG'
            WHEN tok.bank='ORBC' THEN 'ORIENTAL BANK OF COMMERCE'
            WHEN tok.bank='ALLA' THEN 'ALLAHABAD BANK'
            WHEN tok.bank='SYNB' THEN 'SYNDICATE BANK'
            WHEN tok.bank='CLBL' THEN 'CAPITAL SMALL FINANCE BANK LTD'
            WHEN tok.bank='KCCB' THEN 'THE KALUPUR COMMERCIAL CO OP BANK'
            WHEN tok.bank='CSBK' THEN 'CSB Bank Limited'
            WHEN tok.bank='ESAF' THEN 'ESAF SMALL FINANCE BANK LTD'
            WHEN tok.bank='STCB' THEN 'SBM BANK INDIA LTD'
            WHEN tok.bank='SHIX' THEN 'SHIVALIK SMALL FINANCE BANK LTD'
            WHEN tok.bank='KLGB' THEN 'KERALA GRAMIN BANK'
            WHEN tok.bank='ACUX' THEN 'THE ADARSH CO OP URBAN BANK LTD'
            WHEN tok.bank='CGBX' THEN 'CHHATTISGARH GRAMIN BANK'
            WHEN tok.bank='SPCB' THEN 'THE SURAT PEOPLES CO OP BANK LTD'
            WHEN tok.bank='SRCB' THEN 'SARASWAT BANK'   
        ELSE tok.bank END,1,40) as DEBTORBANKNAME
        ,'YES BANK' as CREDITORBANKNAME
        ,UPPER(SUBSTR(COALESCE(tok.account_type,'SAVINGS'),1,7)) as ACCOUNT_TYPE
        ,"" as MOBILENUMBER15
        ,"" as EMAILADDRESS
        ,SUBSTR(tok.ifsc,1,11) as DEBITORBANKCODE
        ,'YESB0000001' as CREDITORBANKCODE
        ,SUBSTR(tok.beneficiary_name,1,40) as DEBTORNAME
        ,SUBSTR(regexp_replace(ba.beneficiary_name, '[^0-9a-zA-Z ]', ''),1,40) as CREDITORNAME 
        ,'Adho' as FREQUENCY
        ,'INR' as CURRENCY
        ,"" as FIXEDAMOUNT
        ,SUBSTR((tok.max_amount),1,13) as MAXAMOUNT
        ,(tok.created_at+19800) as STARTDATE
        ,"" as ENDDATE
        ,CASE WHEN m.category='6012' THEN 'L001'
            WHEN m.category='6050' THEN 'B001'
            WHEN m.category='6051' THEN 'B001'
            WHEN m.category='8299' THEN 'E001'
            WHEN m.category='8211' THEN 'E001'
            WHEN m.category='8220' THEN 'E001'
            WHEN m.category='6300' THEN 'I001'
            WHEN m.category='6211' THEN 'M001'
            WHEN m.category='5399' THEN 'U099'
            WHEN m.category='7399' THEN 'U099'
            WHEN m.category='5817' THEN 'F001'
            WHEN m.category='5968' THEN 'F001'
            WHEN m.category='9311' THEN 'T001'
            WHEN m.category='4814' THEN 'U005'
            WHEN m.category='4899' THEN 'U005'
            ELSE 'A001' END as CATEGORYNAME
        ,concat(term.gateway_merchant_id2,'_') as REQUESTREFERENCE
        ,"" as NEXTSTATUS	
        ,"" as REASONTYPE	
        ,"" as STATUS	
        ,"" as REASONNAME	
        ,"" as MANDATE_INITIATED_BUSINESS_DATE
        ,(tok.confirmed_at+19800) as SPONSOR_CHECKER_APPROVAL_DATE
        ,"" as MANDATE_CREATION_DATE
        ,COALESCE(mdd.MANDATE_ACCEPTANCE_DATE,(tok.confirmed_at+19800)) as MANDATE_ACCEPTANCE_DATE
        ,"" as RECEIVETIME
        ,SUBSTR(term.gateway_merchant_id2,1,18) as CREDITORUTILITYCODE
        ,"" as DEBTOR_CUSTOMER_SCHEMEPLAN_NO	
        ,"" DEBTOR_CUSTOMER_REFERENCE_NO
    FROM realtime_hudi_api.tokens tok
    LEFT JOIN realtime_hudi_api.merchants m
        ON m.id=tok.merchant_id
    LEFT JOIN terminal_data term
        ON term.terminal_id=tok.terminal_id
    LEFT JOIN realtime_hudi_api.bank_accounts ba
        ON ba.type='merchant'
        AND ba.deleted_at is null
        AND ba._is_row_deleted is null
        AND ba.merchant_id=tok.merchant_id
    LEFT JOIN min_debit_date mdd
        ON mdd.token_id=tok.id
    WHERE tok.id in ({});
    '''.format(list_of_tokens,list_of_tokens)
    query=query.replace('])',')')
    query=query.replace('([','(')
    sprk_data=sqlContext.sql(query)
    pd_df=sprk_data.toPandas()  

    print(pd_df)


    base_date = datetime.datetime(1900, 1, 1, 0, 0, 0)
    #Sponsor Checker Approval Date
    pd_df['SPONSOR_CHECKER_APPROVAL_DATE_NEW']= pd.to_datetime(pd_df['SPONSOR_CHECKER_APPROVAL_DATE'], unit='s')
    #pd_df['SPONSOR_CHECKER_APPROVAL_DATE'] = pd_df.SPONSOR_CHECKER_APPROVAL_DATE_NEW.apply(lambda x: dt.strftime(x, "%m/%d/%Y %I:%M:%S %p"))
    pd_df['SPONSOR_CHECKER_APPROVAL_DATE']=(((pd_df['SPONSOR_CHECKER_APPROVAL_DATE_NEW']-base_date).dt.total_seconds() / (60*60*24))+2)

    #Start Date
    pd_df['STARTDATE_NEW']= pd.to_datetime(pd_df['STARTDATE'], unit='s')
    pd_df['STARTDATE']=(((pd_df['STARTDATE_NEW']-base_date).dt.total_seconds() / (60*60*24))+2)

    #Mandate Acceptance Date
    pd_df['MANDATE_ACCEPTANCE_DATE_NEW']= pd.to_datetime(pd_df['MANDATE_ACCEPTANCE_DATE'], unit='s')
    pd_df['MANDATE_ACCEPTANCE_DATE']=(((pd_df['MANDATE_ACCEPTANCE_DATE_NEW']-base_date).dt.total_seconds() / (60*60*24))+2)

    #Request Refernce
    pd_df['row_no'] = np.arange(pd_df.shape[0]).astype(str)
    pd_df['REQUESTREFERENCE_1']=pd_df['REQUESTREFERENCE']+pd_df['row_no']
    pd_df['REQUESTREFERENCE']=pd_df['REQUESTREFERENCE_1']
                                      
    pd_df_final=pd_df[[
    'UMRN_NO',
    'SYSTEM_STATUS',
    'PAYMENTTYPE',
    'DEBTORACCOUNTNO',
    'DEBTORBANKNAME',
    'CREDITORBANKNAME',
    'ACCOUNT_TYPE',
    'MOBILENUMBER15',
    'EMAILADDRESS',
    'DEBITORBANKCODE',
    'CREDITORBANKCODE',
    'DEBTORNAME',
    'CREDITORNAME',
    'FREQUENCY',
    'CURRENCY',
    'FIXEDAMOUNT',
    'MAXAMOUNT',
    'STARTDATE',
    'ENDDATE',
    'CATEGORYNAME',
    'REQUESTREFERENCE',
    'NEXTSTATUS',
    'REASONTYPE',
    'STATUS',
    'REASONNAME',
    'MANDATE_INITIATED_BUSINESS_DATE',
    'SPONSOR_CHECKER_APPROVAL_DATE',
    'MANDATE_CREATION_DATE',
    'MANDATE_ACCEPTANCE_DATE',
    'RECEIVETIME',
    'CREDITORUTILITYCODE',
    'DEBTOR_CUSTOMER_SCHEMEPLAN_NO',
    'DEBTOR_CUSTOMER_REFERENCE_NO']]

    print(pd_df_final)

    #Mailing code

    html = """\
        <html>
        <body>
            <br>
            Hi,
            <br>
            PFA
            <br>
            <br>
            Regards,
            <br>
            Anurag
            </p>
        </body>
        </html>
        """
    subject='Yesbank migration file'+str(i+1)
    #sender_address = 'rohit.r@razorpay.com'
    sender_pass = mail_token #App Password Generated for Email Accounts with 2FA must be replaced everytime there is a password change
    #receiver_address = ['rohit.r@razorpay.com']
    EXPORTERS = {'yesbank_one_time_migration_file'+str(i+1)+'.xlsx': export_excel_yesb_format,'all_token_list_file'+str(i+1)+'.xlsx':export_excel_token_list}
    DB_List={'yesbank_one_time_migration_file'+str(i+1)+'.xlsx': pd_df_final,'all_token_list_file'+str(i+1)+'.xlsx':pd_df_tokens}
    mailer_w_attach(html,subject,sender_pass,sender_address,receiver_address,EXPORTERS,DB_List)
    
    print("yesbank_one_time_migration_file"+str((i+1))+" shared_dec10")

# COMMAND ----------

count_of_run=math.ceil(len(token_list)/50000)
for i in range(count_of_run):



    subject='Yesbank migration file'+str(i+1)
    #sender_address = 'rohit.r@razorpay.com'
    sender_pass = mail_token #App Password Generated for Email Accounts with 2FA must be replaced everytime there is a password change
    #receiver_address = ['rohit.r@razorpay.com']
    EXPORTERS = {'yesbank_one_time_migration_file'+str(i+1)+'.xlsx': export_excel_yesb_format,'all_token_list_file'+str(i+1)+'.xlsx':export_excel_token_list}
    DB_List={'yesbank_one_time_migration_file'+str(i+1)+'.xlsx': pd_df_final,'all_token_list_file'+str(i+1)+'.xlsx':pd_df_tokens}
    mailer_w_attach(html,subject,sender_pass,sender_address,receiver_address,EXPORTERS,DB_List)
    
    print("yesbank_one_time_migration_file"+str((i+1))+" shared_sep19")

# COMMAND ----------

pd_df_final

# COMMAND ----------


