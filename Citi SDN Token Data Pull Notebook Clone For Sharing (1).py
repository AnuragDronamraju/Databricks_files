# Databricks notebook source
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
mail_token='vgdv ygpp ttiv dopc'
receiver_address=['dronamraju.anurag@razorpay.com']

# COMMAND ----------

emandate_tokens=pd.read_csv('/dbfs/FileStore/tables/Rohit2819/emandate_citi_tokens_scored_filtered_Aug28.csv')
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

# V1 Shared Payment_ID input

count_of_run=math.ceil(len(payment_list)/50000)
for i in range(count_of_run):
    

    #All token list
    query='''
    WITH terminal_data as (
    SELECT terminal_id,gateway,gateway_acquirer,merchant_id FROM realtime_terminalslive.terminals
    where deleted_at is null
    GROUP BY 1,2,3,4
    )
    SELECT p.token_id
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
    FROM realtime_hudi_api.payments p
    LEFT JOIN realtime_hudi_api.tokens tok
        ON tok.id=p.token_id
    LEFT JOIN realtime_hudi_api.merchant_details md
        ON md.merchant_id=tok.merchant_id
    LEFT JOIN terminal_data term
        ON term.terminal_id=tok.terminal_id
    LEFT JOIN aggregate_ba.final_team_tagging ftt
        ON ftt.merchant_id=p.merchant_id
    WHERE p.id in ({})

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
    '''.format(payment_list)
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
    where deleted_at is null 
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
            Rohit R
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
    
    print("yesbank_one_time_migration_file"+str((i+1))+" shared")

# COMMAND ----------

pd_df_tokens

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
    
    print("yesbank_one_time_migration_file"+str((i+1))+" shared")

# COMMAND ----------

# V3 Direct Token_Id input

for i in range(count_of_run):

    # Yes Bank Format Data
    list_of_tokens = pd_df_tokens[
        (pd_df_tokens['method'] == 'nach') &
        (pd_df_tokens['shared_direct'] == 'Direct') &
        (pd_df_tokens['gateway_acquirer'] == 'citi')
    ].token_id.to_list()


    query='''
    WITH terminal_data as (
    SELECT terminal_id,gateway,gateway_acquirer,get_json_object(identifiers,'$.gateway_merchant_id2') as gateway_merchant_id2 FROM realtime_terminalslive.terminals
    where deleted_at is null 
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
            Rohit R
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
    
    print("yesbank_one_time_migration_file"+str((i+1))+" direct")

# COMMAND ----------

query='''
WITH terminal_data as (
    SELECT terminal_id,gateway,gateway_acquirer,merchant_id 
    FROM realtime_terminalslive.terminals
    where deleted_at is null
    GROUP BY 1,2,3,4
    )
    SELECT tok.id
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
    WHERE tok.id in ('JMj0c5lbQ0lSQH',
      'JMj22xnDxCm6Sf',
      'JMj3B4xpBQpUMC',
      'JMjDIPG2jrAhHg',
      'JMjFARhRpTyjqA',
      'JMjtFOmljshJ9z',
      'JMjwNCDXK2OVEm',
      'JMjxM4b59HmH4j',
      'JMjxjbWUAqI4Hk',
      'JNTDhQrHJAdc6I',
      'JNTE1xr5z10QJp',
      'JNaiYqJldvFUb8',
      'JNajO8yeVerv3b',
      'JNakStHlyVercj',
      'JNalXlJ6qigui0',
      'JNaleQ5JKtbSzX',
      'JNaoeB3gBV73ck',
      'JNaqa2gdaxn4aO',
      'JNaqkSuk3cyoJh',
      'JNaqmbZCLhafFa',
      'JNasufV5Bu80RN',
      'JNatEYDmWOGIKF',
      'JNav4B17Hg5z13',
      'JNazSqV2eH5XWH',
      'JMj1JKlUn4hff4',
      'JMj5gXmifiX5f6',
      'JMj5hAZYjwJP3o',
      'JMjFTN2FxerCCq',
      'JMjq7WT4QxdSUq',
      'JMjtdCH88ORgMh',
      'JNTC3N2jCgbk49',
      'JNafza90tf28EA',
      'JNakBYHGcayi5A',
      'JNamMIk3RYSwyG',
      'JNanLQ8xdYGJhO',
      'JNaptew6FYlnLa',
      'JNaqPFrNX3VPVc',
      'JNauHWiMLw55VU',
      'JNauNyF2eK5K3h',
      'JNauVzCT1Q6utx',
      'JNaxYoR0ZSfsAU',
      'JNayTQSaRPBcBp',
      'JNb0KNs2Y4sUOU',
      'JNb0PhkTsgF9iz',
      'JNb3DN1bri7hpM',
      'JUf3mfwwDX64VA',
      'Jo0uqhDd2iU0MV',
      'JMizgPd9RWXekD',
      'JMj0ovgvrYYbWj',
      'JMj3BKiDqJauhj',
      'JMj5TvfVImXSoH',
      'JMjMOVPmGcpvpY',
      'JMjr6ijSx7cpcb',
      'JMjvVdyz52QKyz',
      'JNTDI3RTFYKoUb',
      'JNajBLyeLMuaqd',
      'JNak90XIBot7GF',
      'JNalLxqtdYMS54',
      'JNamDDvR0ytWoa',
      'JNamFwCKvWNaty',
      'JNamZLjrixiQxW',
      'JNaoJHS9Gk0jZt',
      'JNas2KvLeMBs6J',
      'JNas6a751gqoY3',
      'JNatAvfNS8OLUh',
      'JNaur3sVAIkQQr',
      'JNaw14AIR9MZJX',
      'JMiyGGe2LlqeTu',
      'JMjIXtOpzhqwL9',
      'JMjL1WOf0JJMvT',
      'JMjvu5PaoVwJcf',
      'JNTCghHRcyhYP0',
      'JNaj3DtXYE45Aq',
      'JNaoMuj4hrFpsn',
      'JNaoihtZS4dUQj',
      'JNapLghc1MWsYl',
      'JNaqBzQUwVWoZD',
      'JNas2FmqvqLQcc',
      'JNasFkGLj76DIR',
      'JNasJ63s2fXm85',
      'JNawSWzZf73tsV',
      'JNawYzMrIPBzrb',
      'JNb086vEdRKTOb',
      'JIHyNsY6sbRyrh',
      'JMj2LTvA9JP5cU',
      'JMjKjiYynOhGyM',
      'JNTC9SJqVKszzK',
      'JNam5eNottgGcG',
      'JNanzzsNrx5WiC',
      'JNapbXBsnRWQqe',
      'JNasHtkcSEAMHB',
      'JNat8OuIBbTscP',
      'JNawo0tsqfbhhR',
      'JNb0fzGl9VUo18',
      'JNb22o4krBcVOk',
      'JONvTF8h5koobZ',
      'JMixdIonAS9UyN',
      'JMj1nstIxGy3Rf',
      'JMj1wKGhY0iNgH',
      'JMj2Ah6HnFULHB',
      'JMj4KHUbce9P4t',
      'JMjq4Fx8s6s03o',
      'JMjqZm2lboq0Nd',
      'JMjqc9CJiyxnUK',
      'JMjsbZlD5mAbpF',
      'JMjuF3qTkBsVHT',
      'JMjvkW6EFk4gAH',
      'JNaeFyzt5kYats',
      'JNajk0xegq8Oh2',
      'JNalfRp1DsWVrj',
      'JNalu1BtSjy6zA',
      'JNanDAMwivzFCj',
      'JNand2RznMBEsA',
      'JNaoXGh7nElkv9',
      'JNaq2GpheJWhGb',
      'JNaq9P9n8kgJRF',
      'JNau0TkspHKr62',
      'JNb0x9kFGDg4Hj',
      'JNb14TrXaUQXjN',
      'JNb1i21c6Ykr1S',
      'JMj0dgMfqvTi3P',
      'JMjFfY5rGnEWIT',
      'JMjHVRhXbiyE94',
      'JMjIKhYQrzJkJA',
      'JMjL56jJudpB1C',
      'JMjLcvw69yCtVY',
      'JMjsnZpc2Y00WR',
      'JMjui0xhEh95hE',
      'JMjwDDzun7kWnA',
      'JNajKaz1QhnOC0',
      'JNajW9GNVCiC77',
      'JNap1veRZwLGpT',
      'JNap69Byo4nejs',
      'JNapVIBKIqKKPt',
      'JNaqM4HQVSL9XU',
      'JNatxYe80Ot79H',
      'JNaurH4nRnzvrj',
      'JNax2JY1BDLkPI',
      'JMix1utYnV4oBY',
      'JMiyXpH17UbT2K',
      'JMiznoBrzuIsWb',
      'JMj1zL8CRHBqe3',
      'JMjDCHrJbwFF5J',
      'JMjEXUW17vUIqY',
      'JMjrrWcN3eBrc0',
      'JMjuW2y3GEfHKf',
      'JMjzIuZ2Jll5A4',
      'JNTCyJgZvjRjSQ',
      'JNagpMH4yBsrEj',
      'JNahRWTX8eU5Jz',
      'JNajjv9Sdbls1n',
      'JNamj6I9cpKFiW',
      'JNapNG69nOL9Mn',
      'JNapZPC6pPELWD',
      'JNas7wqOA0p3ta',
      'JNasdyJFi4LSsg',
      'JNaxGz3nXcCxZE',
      'JMj2Qy1nFvaK1G',
      'JMj3N5RUQkO2jJ',
      'JMj5OugL3eBdac',
      'JMjDX4gN1ud004',
      'JMjEN578zWcexY',
      'JMjtKz6bDsrhxT',
      'JMjtqGGlipkeup',
      'JNTDSVB3cfzUZd',
      'JNai1M9Nb8SLv6',
      'JNaiGQt21ZBs8U',
      'JNamQV4oE87yn7',
      'JNamXP4UfNfQpm',
      'JNapKWNFoaUOhr',
      'JNapqcZjb0XrFv',
      'JNaqE42urF1sFK',
      'JNarA9UJJPvzMK',
      'JNariTuMlbc7ev',
      'JNasEMVqSvGbdZ',
      'JONvNhtkHINHao',
      'JMiwjrQe1ni4El',
      'JMjvl5DtNe2oVy',
      'JNaguYvcvYn9in',
      'JNalAvFcXaKcX7',
      'JNanNrCPE4njnm',
      'JNaq16pkPTVLQu',
      'JNaqBD4Fjzdux4',
      'JNaqs0vbrSp7o3',
      'JNar8IGJ34To1s',
      'JNataJO0eGQqnf',
      'JNazO2gzgIgYk7',
      'JNazZzDsX85Czd',
      'JNb1GIZpDS8Vd8',
      'JMjHPfnM3Qpmw8',
      'JMjKtR6Vghzewc',
      'JMjuTWjyXQFSrM',
      'JMjujtHagQdCzI',
      'JMjwKeCTQY0o97',
      'JNTCFI5YNv8eUS',
      'JNTD6PslWoPNTp',
      'JNahEZvH7kSrkf',
      'JNaoS3ajawx2w6',
      'JNapyTu7qf1NeA',
      'JNaqTZ3UgLzcVO',
      'JNaqlKT7doRfyK',
      'JNarANnqlYZu2A',
      'JNas9T4mscRs4E',
      'JNauESucPhdWGd',
      'JNazCEkUKnTN4Z',
      'JNazI0QoTGtngy',
      'JUEkSaKuDKUpci',
      'JMiwydxVgLXhxa',
      'JMj1ZI1AmY2FPh',
      'JMj2dkXBvxFtUb',
      'JMj4JWYu98yWqE',
      'JMj5duQIOPdIhP',
      'JMjGgLwqvo58jx',
      'JMjHXJZ1wCWhDy',
      'JMjI3Z9fGhNJjn',
      'JMjKMWpK44kTak',
      'JMjsSclkB8nU6F',
      'JMjsfAUQdM7lCv',
      'JMjuySTxsgbt41',
      'JNakBaDDkJofr1',
      'JNakcztACQH3f4',
      'JNalAdnosgolAz',
      'JNalSlkRuADksE',
      'JNamKr01gfFCBI',
      'JNammwPQlc3vep',
      'JNamocCBfOxDF5',
      'JNaoTiIIlJXHBI',
      'JNaptjK5bjUpQL',
      'JNapyNXzjLSfEy',
      'JNasF2jNdqMjzC',
      'JNatbqGJO7i2my',
      'JNatiIMQKDk0LU',
      'JNaug6XrePxMc7',
      'JMj2GC5nCheafU',
      'JMj3gy6H0dUYkS',
      'JMj5CMgqazoAMW',
      'JMj5NLWQnIvDt3',
      'JMj5uRALDNqEro',
      'JMj68JPQfLI5Oq',
      'JMjJl6kDun7OcY',
      'JMjsGiECVrtTpW',
      'JMjtScAkMe1AQZ',
      'JMjwVU5iFzomdy',
      'JMjwWNj9jhVFai',
      'JMk01GmUGvYNwf',
      'JNTDq1WxlmBrTU',
      'JNajLAGiupCqzU',
      'JNak8noTOVYnTW',
      'JNakKmfkylPGTk',
      'JNakqVh9sBD9GW',
      'JNal5yMeuLGpK8',
      'JNap3ZhEcQo33B',
      'JNapYjntsTxRhP',
      'JNaqh2E3OvN4pf',
      'JNaqzzG7G51f7e',
      'JNasUHWNs5UwO4',
      'JNasvrf6qBqbvS',
      'JNatJKNPtz0bn3',
      'JNataWNn5H5qU9',
      'JNavko2ALBXrZM',
      'JMiyaqfGFuvaJD',
      'JMj4GTcATaouA0',
      'JMj4V43pvSY0z5',
      'JMj5uMNQitOvuE',
      'JMj5zaeT6Jf2wi',
      'JMjEtp9mRYS2qt',
      'JMjHY9xQXkVykE',
      'JMjICac4u9qtg2',
      'JMjLQJqFS9sAvE',
      'JMjw66aCQWUT56',
      'JMjyAvHMu7kC8x',
      'JMjyJ1hx3uwcOz',
      'JMk07yfxfopTri',
      'JNak6BCSR0wXcC',
      'JNakvqbARpha44',
      'JNamE9frDQl8cm',
      'JNan2MhvHg8f2I',
      'JNaoDxTMUN2mzs',
      'JNaodMJpVv5pDC',
      'JNaq9qxVYKIAOZ',
      'JNaqD2i6MCFDPx',
      'JNarIGAYFHGyq7',
      'JNardX39qnlPxq',
      'JMjHHxuDTzMMK1',
      'JMjIpekgt1VAQm',
      'JMjqRbuBG68ID4',
      'JMjxEqBEkCBPCI',
      'JNTC6bpAwy8cT5',
      'JNaerB8fgl33Hm',
      'JNafAtBQB1FHvG',
      'JNajjAsBxQc6BY',
      'JNamNja0piMAdJ',
      'JNansYY8T4WmqL',
      'JNaoXKVHZ1TlHG',
      'JNaqe3e2L8tPkR',
      'JNarjVcjxYbCI6',
      'JNat58bpcf1h5A',
      'JMizY2quAXWb4Y',
      'JMj0IqiigGHzN7',
      'JMj538S55joTB5',
      'JMjDjnA3QAZelW',
      'JMjIoVfevwOv0z',
      'JMjsH0vzDkYWUf',
      'JMjxUrXGUHPiIN',
      'JNTDTyBfuWarzP',
      'JNTDjNCy6zQtip',
      'JNagk8jR9KetiO',
      'JNakQCowKRbM3C',
      'JNan9oiI04aVOF',
      'JNaoIzxh1zd3OS',
      'JNaojAOwMF4nLL',
      'JNarXHjQO4c8ir',
      'JNasHM1KB6E8dW',
      'JNasVYeVp41jd3',
      'JNavYTktYDCIYx',
      'JMj1LQIqjafYdj',
      'JMj4QcJKydTkfI',
      'JMjJerh5eDCQli',
      'JMjMLvyZvUWUxv',
      'JMjvfBdjhv4EjK',
      'JNaisdKygDtdpv',
      'JNal0fBY3ogEVU',
      'JNalfpR5PDHPJh',
      'JNalngmkPt6rAc',
      'JNamAHUrscU6im',
      'JNameo5G3bCsm3',
      'JNan9iGWTZ8hSU',
      'JNapIh8kvFav4a',
      'JNapSGbTx1RuB3',
      'JNas2xQDjpCwKs',
      'JNau9PCndQ6peL',
      'JNazj3Xdtuomj5',
      'JNb2fRgvIlNvG9',
      'JMj2RLNzGyU4gJ',
      'JMj2RipxAUfoyJ',
      'JMk03AIkgJpJnP',
      'JNae2iGzjhFXLf',
      'JNafID1Aj5Fn08',
      'JNamk0FP91PTuq',
      'JNamnu5qruS9DH',
      'JNao47zbArue8H',
      'JNaoLrxZSR0pfb',
      'JNaoPDqmynvh7E',
      'JNapAd1gHMEmtX',
      'JNapQViNX6b81N',
      'JNaphEKP8fJgQP',
      'JNaqsaDcr1gXjR',
      'JNarMzsKMDCPY9',
      'JNateeniPneJLr',
      'JNauA390t6clmp',
      'JNavARbskON4eh',
      'JNawpDJNLWRKWD',
      'JOmRTDx7pe36cJ',
      'JMj2yIdCmT5buP',
      'JMjDlnj0Ey3RSf',
      'JMjFkKsdWoj8xw',
      'JMjq0Zg8QXOalh',
      'JMjsyJ1ke7SWu5',
      'JMjtsFFlzITNDL',
      'JMjw8yC27CV2Za',
      'JMjwKAyt7uiFl8',
      'JMjwbsv4RI7qSF',
      'JMjxwNkNh56f9O',
      'JNTCKh40IZdgji',
      'JNTD6QX78QFMPg',
      'JNahxUyRWxeFeW',
      'JNaia1roq1ck1z',
      'JNajcp2YMFYwmD',
      'JNakDrjZ3xIQEB',
      'JNanV5lLyMfOSd',
      'JNaoVFN3yHjH9R',
      'JNap3Uz7fdiVR8',
      'JNaqWLPF9gMpLE',
      'JNaqaky62Y8YQm',
      'JNaqkSxJZYFKgu',
      'JNauXcIxA5xkyT',
      'JNav3LEYQhut1R',
      'JUf3wFvH2hU4vB',
      'JMixXfo4DeZl81',
      'JMizdC9r1IaMkU',
      'JMj0P3CYp3VI7t',
      'JMj2IMExjhXXrj',
      'JMjrNLCEwdhz7t',
      'JMjteaDT5dqcNk',
      'JMjvvhTL0iONEO',
      'JMk0Pc0L4s2THy',
      'JNaepChvHqwBb0',
      'JNagHCI3J4hYdh',
      'JNahbtirzgqUoK',
      'JNajINRxKgEZ4r',
      'JNalyvavphlTiI',
      'JNamdAxhHFqew7',
      'JNaogpkx7ShFX7',
      'JNapDnQ5uJKw2b',
      'JNapoAJ2iKehHx',
      'JNar0rSVNnDNcz',
      'JNarK6M70BJO1w',
      'JNat8SCp0MjiAr',
      'JNaw8VbCfKtv9t',
      'JNay8BtgBlodPk',
      'JMj5KLdPpOhE1T',
      'JMj5mqK3HAmDzS',
      'JMjCwVwaftFxFn',
      'JMjrSfmOUXK9Uj',
      'JMjy3gTn5PajVw',
      'JNTCRxepUyM6pn',
      'JNag0sNdCCGXtv',
      'JNajewVVAKxauv',
      'JNakBTbwbyXrqY',
      'JNakhdKodU5r2B',
      'JNal3PrYtrYHab',
      'JNamf2mKcruhns',
      'JNanFwM2DQ6Wyu',
      'JNaoOjN6Sp15Jr',
      'JNatfxaiVLCLx9',
      'JNatpqyoTvTtXW',
      'JNb09qwyAyfP6O',
      'JNb1uVuqz6zo5b',
      'JMixSEYSIxK4dK',
      'JMjFffZBUcAEpj',
      'JMjHTW4KohUXLd',
      'JMjJiIkqgsjZ4J',
      'JMjL4p36PKEpz0',
      'JMjuLN9jOdwofl',
      'JMjuZnnFCaLz6g',
      'JNaeruRg0Vu07d',
      'JNajIgNfwik27N',
      'JNalqkfRENN93f',
      'JNampmP6ZqoW9E',
      'JNanCfBTO4cvAZ',
      'JNanONHJZthgQI',
      'JNanUhss8XbLXe',
      'JNanh8cjy8udU2',
      'JNao1zwvKKRhCc',
      'JNaouKgCqRvEEh',
      'JNapEPNDJJecda',
      'JNaqdV4im4tz39',
      'JNat3zvUALAZlY',
      'JNb0704Fvi9EAz',
      'JUEk32cwmCkafB',
      'JUf3pMst07BaLk',
      'JUf3rgqdza5QKx',
      'JMiztsQCO3hUqz',
      'JMjEEQOOa3GKuP',
      'JMjFVUy26rI4Pw',
      'JMjGB3iIObDCpL',
      'JMjrgqbC1DRVay',
      'JMjrqwkq5ue8Ig',
      'JMjuQximvay3SQ',
      'JMjweVTfXIK6f4',
      'JMjxmP5NeiY5gk',
      'JMjy01LzJDyRLm',
      'JMjzXngS6rGOfV',
      'JNagrZ0loIktc5',
      'JNamkxyIz6KZ1V',
      'JNamoTqXpJAN5L',
      'JNankOIeuqClxn',
      'JNapVvKiX65a9F',
      'JNaqJdZMrbNoi3',
      'JMixFQtTZ8Acgm',
      'JMixfgcbbHqkxC',
      'JMj1EHch4yz3oz',
      'JMj1hTd9jot4GR',
      'JMj5bQBvKhyRKl',
      'JMjEqy2nBHlfQO',
      'JMjtY0hGhq6Nk5',
      'JMjtfzjPsfSC1o',
      'JMju3D2WLOzD8E',
      'JMjuqogX4PZ3YI',
      'JMjxmEVIvwvDQE',
      'JNaeHB8TjJjfMi',
      'JNafnbaF3XZ9JB',
      'JNam7BH1iJXg3n',
      'JNanvRWPp9NyzB',
      'JNaoLW3NubdOu4',
      'JNaqozs92w5XnO',
      'JNatzm9AB3jmIk',
      'JNaveK7fZKtlnF',
      'JNayPO9nQb6vbB',
      'JNb0vhLRAWIEwa',
      'JNb127kfKKLU1u',
      'JMj4a9QAF0T1b4',
      'JMjFKjsdexE0yo',
      'JMjGyL2jAjfBxe',
      'JMjHtrZpABXG5d',
      'JMjrg5IVeeIkXs',
      'JMjt1MGbK3yGDR',
      'JMjtZrBq9Z9Stw',
      'JMjtr8X10LIVg4',
      'JMjv3il9A2Blzg',
      'JMjy2LTuFoSBIk',
      'JNakDMH9eF2CT4',
      'JNakV9V1QVrg4Q',
      'JNalCgfSYVbFac',
      'JNalI9gUIgheGQ',
      'JMiyv7IKOk4KpA',
      'JMj359vmUrpmLV',
      'JMjIN04XzWdbic',
      'JMjJQoPQLltFcO',
      'JMjsx4VTkMczPZ',
      'JMjtUVt4dv7PJc',
      'JMjxisfXkP3LCT',
      'JMjxrf6BJU1sSb',
      'JMjzuAtRNIw1s0',
      'JNaiYrKbh8oIdJ',
      'JNam2gyZCw5cFY',
      'JNam7K7nJfDWbF',
      'JNaoQYpEUgnez6',
      'JNarLbwe0iZW5D',
      'JNarxRTwHzwDJL',
      'JNb2oodSQzzv9W',
      'JMiyz0mbjPZupI',
      'JMjFxLhnXJRUEG',
      'JMjJ3fZYPn1Jpc',
      'JMjKmQuh0C7Al0',
      'JMjtYELsFXT5Yv',
      'JMjtcuSrqVrf3V',
      'JMjuYPjgTHTKDr',
      'JMjvP2hA37zLFZ',
      'JMjwa74IUtOhY2',
      'JMjxe89ld7hxzf',
      'JMjyA1JTPxXaAC',
      'JNTDGCGEz0hi2j',
      'JNTDQtjveUuYEb',
      'JNanCpbipTTjqR',
      'JNaovxIMydSZyQ',
      'JNaqSGr4qHjvNa',
      'JNasjP7ZhIQfkX',
      'JNatEat2YAABFN',
      'JNatxynGlt5pdD',
      'JNavWvbTRQdHXC',
      'JNavc9BJVUE8SZ',
      'JNaxZYKM4WoUwL',
      'JNazltPfV64vFF',
      'JMimufrfhueCiY',
      'JMj0kInbHBK9bf',
      'JMj0l7IGDz2yAD',
      'JMj4fYLCqzbuny',
      'JMjEZQdyJLArFH',
      'JMjFhHNfU9nXzg',
      'JMjq9r7cCMojnf',
      'JMjsosOYzyGUbr',
      'JMjvYC9OPrU8QF',
      'JMjwHhKX4OnTog',
      'JMjym7dp7XneJW',
      'JMjz1r8WpRSEk7',
      'JMjzC0SyKEAUVz',
      'JNaibn0JFLk9XP',
      'JNajVX94EkrF2L',
      'JNakrxQThBPH0T',
      'JNalFEMSW25U9H',
      'JNam4LCcHI6Iln',
      'JNamVeBvqOO50r',
      'JNanedZcNsV9hB',
      'JNani0Cyd5tnK7',
      'JNanupEoH4uHzm',
      'JNaqly7jLsm8Ln',
      'JNasXwUsFiMtrr',
      'JNatcZ55SZpJSx',
      'JNatnW07DCKBQY',
      'JNau4bZXcExJEP',
      'JNauCacu129uMa',
      'JNav338jzStrvL',
      'JNazAbJyENi5tj',
      'JMj3UR6a737qTw',
      'JMj3s8GIKzd8jm',
      'JMjH7PDrgYbUBL',
      'JMjJRAlWjT6vvG',
      'JMjt2q7z8VC9oI',
      'JMjtUNNZiuvJUX',
      'JMju5k0fE2ZeqT',
      'JMjvzn9UqOkadM',
      'JMjw3Z4PTsfbnd',
      'JMjwBeIDfy1M2q',
      'JMjwsJeX8NNjMa',
      'JMjyQc0NLjihn8',
      'JNai53tEivoEyK',
      'JNalZwVK7SzrJj',
      'JNargjbbzuweHA',
      'JNawpSZQqXWygu',
      'JNb0F2ZdtyuLC5',
      'JMixFPXzUnBMGo',
      'JMjGTiXauDerpr',
      'JMjHdefYSwbtFG',
      'JMjLypwKb6z4FC',
      'JMjM0Xsfz1u6kU',
      'JMjsbNfHB9Y4Va',
      'JMjtPK7okuLfMH',
      'JMjuAjcAVn03Ly',
      'JMjwLczZTKFAGW',
      'JMjxfmzHJOxpNd',
      'JNaj6Wxj7Dcdaa',
      'JNajlZqcGnXf3i',
      'JNalAnUwbD8sss',
      'JNalCA5l0wtHkp',
      'JNalZR5XL8Bq9d',
      'JNalapKJnGXuLq',
      'JNamQ9bGLNp1NM',
      'JNanFhrHIA8sdk',
      'JNanOXHqIT7nEJ',
      'JNaoBbJeCaIGlz',
      'JNaoyGtZ8pGeWD',
      'JNaq7jZGuNgP5H',
      'JNarFKgWZ2MEBr',
      'JNarTdau2OE1dh',
      'JNarrPNZw3bhtS',
      'JNau8lDYyBUIR1',
      'JNax4tubwNrRyh',
      'JNb0JIY4svppfO',
      'JNb2PE9sVbZqqc',
      'JONvQZfR2JBwhF',
      'JMj0OSkrQN3Mq7',
      'JMj3enGEZTAHaZ',
      'JMjE4Doy64iu4M',
      'JMjM8npLQNgnxD',
      'JMjsNDcopn7rad',
      'JMjsg5DhhDuHsy',
      'JMjx45W6cHVkDp',
      'JMjy6NAIqIFqoy',
      'JNTDsN3Bm5VzlL',
      'JNaf4P59fiWoy1',
      'JNaffZpHRgusBK',
      'JNafnHlI1t9jP8',
      'JNah4PvpWAvGTp',
      'JNahjL46PRL7d5',
      'JNaikvrEIthI0N',
      'JNamXbPPLgLFXB',
      'JNaoblSEj9uC3r',
      'JNaoy9NsRwD3EW',
      'JNap4yHyESK0on',
      'JNapiwIaaDcHXy',
      'JNapoUWqpDbBnD',
      'JNar8Y3aQrrZwQ',
      'JNarOmhbfGXiiR',
      'JNaulGuWBnYiFj',
      'JNazKIwfXrkA9C',
      'JNb2XSGSaloO1j',
      'Jo0uvReN7x3eSo',
      'JMiwdu9UvNNNEN',
      'JMizPVdW1Uifov',
      'JMj4dklVEuYGuc',
      'JMj4eyBHOXzYc6',
      'JMjK4OJRBYwAFM',
      'JMju86iWL8zcn4',
      'JMjvBbi0Ys70px',
      'JMjx6ZI7LwmCdd',
      'JMjyphz4oZoM30',
      'JNailPsOxzVeQQ',
      'JNak1iNcVVbV21',
      'JNaniXk0ZvNYPm',
      'JNaoVMqM8LKqD0',
      'JNaqnrWyyDmkWE',
      'JNar6tANzOyJSH',
      'JNarLyV0m5cztW',
      'JNas0vCm4CzMAk',
      'JNaslTAysCzWNs',
      'JNatVmxIZstpuX',
      'JNavWCwdbl1ieI',
      'JNayABTqPpkqjS',
      'JNb0idFZXtIAe3',
      'JNb35QFqjemwCj',
      'JOmRnNYZAMObpE',
      'JMj4qn5Wnbdljn',
      'JMj5niumORLAkv',
      'JMjEZGt2qdUaaL',
      'JMjHNfE2oQVkl5',
      'JMjIz7lfFqVVSt',
      'JMjKIKCbGdgzcO',
      'JMjryFg0Gdm7mD',
      'JMjsgw0b0ow0PN',
      'JMjv4PetlSxHYg',
      'JMjvZ9JPjRnHLm',
      'JMjwUmDtiBQfMO',
      'JMjwuE610P97U4',
      'JMjxDwIY3PuAjR',
      'JMjxuIzFyomEXE',
      'JNagIACgpP4aHo',
      'JNahnmcCYcsl16',
      'JNamLMpBecUvuD',
      'JNanY0y3FZDWrW',
      'JNanaaHS2ssoYG',
      'JNaqMbGj0Z8Ok5',
      'JNasedmBYxDgJN',
      'JNauZcxgETogkT',
      'JNawKTMFDuHJwJ',
      'JNawVDiBI3hebv',
      'JNb0XsPZh1h6Dv',
      'JNb0vxqLJRrkew',
      'Jo0uz7gzqZpS2N',
      'JMj2PGAv33oINZ',
      'JMj2ukESpYgzIH',
      'JMjMeuTr4e7uYh',
      'JMjvOSWkPtA3Tr',
      'JMjw8G1bCE5srn',
      'JMjxSR7qcWRjet',
      'JNagFUugx3bAnM',
      'JNahPmtSgnFYof',
      'JNak2vdVrSmv2I',
      'JNakJnSjBB9QAE',
      'JNalwpAnd554xp',
      'JNampoxJUUGXep',
      'JNaonHeKZr5aXv',
      'JNapJDHQ2VkyOP',
      'JNapx64EhvRfYt',
      'JNaq8eyCNmqlgU',
      'JNaqLNxQxwSgnB',
      'JNarDN3w4bex08',
      'JNaswPlJV3wKEN',
      'JNawvfD5nEwbnI',
      'JOmRhAV57Zmw9C',
      'JMiwg1rdVY8DRw',
      'JMiwnrdD35Ikd7',
      'JMizMpiuQlyVve',
      'JMjrSFeiX5qtqo',
      'JMjuVQMJ3RAS3b',
      'JNTCwXOcyUUksG',
      'JNahoOvMpBcJcx',
      'JNai4yI0BPIACM',
      'JNal8jtXUkiPis',
      'JNamDMxdL9E22W',
      'JNamoPA1Gk4h1w',
      'JNaoHJk3ieYVKx',
      'JNaoVSmUp8meMw',
      'JNaq3GUmkEOpPP',
      'JNaqmJJCgZZsJx',
      'JNarqXqhXLX23U',
      'JNasWRi1P43XeJ',
      'JNaztWfndXlIsH',
      'JNb0SgSA2GugMV',
      'JNb1no5gzt1Xw7',
      'JMizD23bLTh1m1',
      'JMizeDKBvT594k',
      'JMizuwP0TkeQM6',
      'JMj1tTgXtiEBir',
      'JMj2vdW6azvIgY',
      'JMjGp7jGbAofXj',
      'JMjIF83bS0bp3y',
      'JMjrZNzbh4h7Gw',
      'JMjvAxnd4OripN',
      'JMjwE4KQqta1SF',
      'JMjwG9PVo1x2vy',
      'JMjyIVTRCwstNq',
      'JMk0WZ5l8z6c3e',
      'JNagxyBHQXiJ40',
      'JNaiy6nJmR0lvP',
      'JNajWh4sSK8PvB',
      'JNalHEyVHxomON',
      'JNaqQOaNzia4O9',
      'JNaqd7BYvcaeZF',
      'JNaw3OWDbgj6wD',
      'JMjDn70V6r6pc6',
      'JMjHkaMwxjq15k',
      'JMjIeY1HOonrWB',
      'JMjJqvuulZhUvh',
      'JMjscgBOpkimiu',
      'JMjtFv2wWP7Ua6',
      'JMjup2Ld4eSanv',
      'JMjvVRuU3X6wbI',
      'JMjvrIMVK0kQgR',
      'JNTDFUB70wepS6',
      'JNagEdeg5yAvje',
      'JNakdrXkJKwuHb',
      'JNakfKm1U2N8aq',
      'JNaldPdcm6J9RH',
      'JNamNWXYKnGfyN',
      'JNamaA4bfHD5a7',
      'JNayrfv8MRmvz9',
      'JNaz2aSwTYFPPL',
      'JWyCcRsvG5lKMr',
      'JMj0htzHGf4Zgc',
      'JMjLIXEB6ysE5o',
      'JMjsJnYbNSr7O7',
      'JMjtkiZgQkWuok',
      'JMjxPQd60ZVIoM',
      'JMjxRO63QAdTCF',
      'JMjyu9F2Hmzddx',
      'JNafl8GXWgo6vo',
      'JNafzSAkBysr9h',
      'JNajqa6uZmanmT',
      'JNaljhUj6sVjbR',
      'JNao62H6Ce7crG',
      'JNapCEX9vgPEe3',
      'JNarEXmnHeSOt7',
      'JNavFRNM9SEtTe',
      'JNavk0axxCRV6o',
      'JMj5ThmxRUjZ6d',
      'JMjqGaZr1jhct4',
      'JMjs02jXJbIKDd',
      'JMjtdCWe9ctQZJ',
      'JNTDsUKKVSULov',
      'JNamHi5JOfHFY9',
      'JNamr1c1aIZ4RS',
      'JNaoNrckPuyb9Q',
      'JNaozNsbJgsgRJ',
      'JNatbj2vAL9pQW',
      'JNaulCilrywAIK',
      'JNawG6Phzo0aa3',
      'JNawwXPKgCsh4y',
      'JNb1RjNKMEHewL',
      'JUf49mUF6E1oJh',
      'JMj4BZwGEdiK7j',
      'JMjIDRYo3DxKQT',
      'JMjv3Rsx9p25z5',
      'JMjzb7hTlQFHV5',
      'JNahhc62tS4MNB',
      'JNaijDIoNbDVbS',
      'JNakP44DE7D1xU',
      'JNal2w0un954Vo',
      'JNam47NvX4N5Bt',
      'JNam5OCOWr0vTj',
      'JNannB4sO9DIyX',
      'JNaqeJM93y2qVi',
      'JNaqxzKfZO3oxB',
      'JNarw0En03BNTu',
      'JNauLapDd3C6Je',
      'JNb2d6dTl7r3To',
      'Jpb6aVUIAsQr9s',
      'JMj0jFyMAwwJ0J',
      'JMj2ASdB3YqLCU',
      'JMjDEwu2YHFAht',
      'JMjFcP4r2CS4PS',
      'JMjtRISGZDXNjw',
      'JMk0TLWthqPTa1',
      'JNTD66VV8hgUhr',
      'JNafHWcXtayW55',
      'JNakCHEj34FRsS',
      'JNamHTNdHMOAth',
      'JNaooXTVpB6Koi',
      'JNasga5P4x2cuw',
      'JNaymoi8TBotVI',
      'JOmR6UN7OWZEoU',
      'JMiwvETeBu8neM',
      'JMiyyzDTkHL8Gc',
      'JMizMPyFvhQkMy',
      'JMj2bw0rKswgRX',
      'JMjyIXtR7KOcNs',
      'JNTCHyX37le7cy',
      'JNTCgdyJauUZI6',
      'JNak9a99oH8wFq',
      'JNakHnBpCt31sL',
      'JNalCT1ZVVZb8a',
      'JNalWI6nGL1OgF',
      'JNalkXnaY31tDK',
      'JNan5eIBStDKca',
      'JNanCpZARxDVUJ',
      'JNangBVyWdi2Ct',
      'JNapMMnaszyIU1',
      'JNaq47mB4yBnrU',
      'JNaqcCf2UnMSou',
      'JNar8u1hg7j9iH',
      'JNatmxAM7K6aT3',
      'JNatrBbns5sdsi',
      'JNavoF8CGqbYIT',
      'JNazH8PeipTy0k',
      'JUfAyEegvDXUCg',
      'JMj3emM2R9ZbMa',
      'JMj5Kwa4jvzu2x',
      'JMjCvG9OEZPYv2',
      'JMjDAVF3ZnUDFP',
      'JMjtdLqwyf2xjc',
      'JMjtvl0hJP9GWj',
      'JMju3lqwfPVHY1',
      'JMjujV0UOvMbq1',
      'JMjx3UqHbemBsT',
      'JMjxN0wFJ0JawA',
      'JNaecQUrGMxrKw',
      'JNahnF4jWvpVLs',
      'JNajDnS9PWZfj9',
      'JNakgQXqjzQlzB',
      'JNal8opNWiH7fr',
      'JNalFHLmBMJOLA',
      'JNalVPW6uWGgiU',
      'JNalosP5BhqwBY',
      'JNangPCMlUHyp9',
      'JNaoN4pFIQ8Tdo',
      'JNapPbPpBNVCIM',
      'JNaqJZap5jq0I0',
      'JNatYJhIBbhwHQ',
      'JNatjQvywUr6TK',
      'JNav7USJxqf7sa',
      'JNaytweOphgvRE',
      'JMjGQ4Y6h8TbCH',
      'JMjJ5YpV6lyvTF',
      'JMjJ9ZAvIX44cx',
      'JMjL9rL6NQ6d8e',
      'JMjMWUquWv6TkE',
      'JMjsvD3mUbBqh2',
      'JMjxtvbeUtU2Td',
      'JNTDNspK2YWwqs',
      'JNaeGPCEIq8q8i',
      'JNaj8pOUvxE7Wb',
      'JNameYI2mR3p9A',
      'JNamv8pY2gERVe',
      'JNamyKXnUZTXp0',
      'JNaoEJbSnHLeuv',
      'JNaoIR2bweydBJ',
      'JNaoQf2QwJJZXx',
      'JNaopmYmyafc5Q',
      'JNarVHNGpBcnVf',
      'JNasncnZ6sXrxZ',
      'JNasv6YUTUhEZU',
      'JNaxyLemYVrZjs',
      'JNb1QoiOZTWAJR',
      'JMiytZEyv9T1a9',
      'JMiyvT1tgW7qDc',
      'JMj21vzfBqie4Y',
      'JMj5KPySs5y2Vj',
      'JMjIXaDoYtoltq',
      'JMjIoHKdIUgtNC',
      'JMjpy8KV3vn2oA',
      'JMjybBLXJIIpg7',
      'JMjzH9edPHQesa',
      'JNagDZJbwfj7Es',
      'JNak1mbLI70GUM',
      'JNaliBC4QiKu4C',
      'JNanENQzjlOm3H',
      'JNanF2uMWKItRB',
      'JNaopZp19KY1w0',
      'JNarf8QkeGZcwC',
      'JNasl50u8EFXWE',
      'JNatjb2K6XcIjf',
      'JNauBAzsqpUEc0',
      'JNauBcQoIhuRaf',
      'JNauK6LFsZJygj',
      'JNauS1TJeiQJ0x',
      'JNauTM1dj5MfQx',
      'JNb2BI7h96l5gH',
      'JNb2MiCIJ3ILko',
      'JMixAmKT3rovJo',
      'JMiySBYn1L40fN',
      'JMiyk8LhyFrdKf',
      'JMizG3JYYjFAYD',
      'JMj1Rd2McaBykd',
      'JMjFoz30XQXabd',
      'JMjIqtNHPaVEcy',
      'JMjN14HiYVJFwX',
      'JMjqheiVqxXaLA',
      'JNafPFlJZ7pjYN',
      'JNalzPgobxjyGx',
      'JNam232Fl3woh5',
      'JNao4qmDMSfmEA',
      'JNaoBggNh1ZQ9P',
      'JNaqgPq1l4bDmz',
      'JNasdNkwlLUKr8',
      'JNavyvtaqtldyX',
      'JNb1s6D277m6YM',
      'JMixuW7rmZuh2X',
      'JMiyTmJ5FQZEu8',
      'JMjDY3cUn2oExL',
      'JMjJBBivBwJSsg',
      'JMjsLNgp0bFZ6V',
      'JMjsTLYRWHnu27',
      'JMjsTznEDlbJJB',
      'JMjvQd1ly8dqMH',
      'JMjyJFegpIue8E',
      'JMjyNRvnoUIrXF',
      'JMjz4yRJv53TY9',
      'JNakTolSzZ1yoK',
      'JNap1bGQV71plo',
      'JNaqDUU4NFKiHw',
      'JNaqsVj1lOvWLz',
      'JNatEIoEGeGbxm',
      'JNayekoY0dUwLO',
      'JNb1doNzPahPhv',
      'JMj0iRNvAdc9G2',
      'JMjL5pVh4N4wKF',
      'JMjLNFXp5EyWMd',
      'JMjMDDh7j40i2j',
      'JMjuLHBbjWsTKd',
      'JMjuqvzkDbE6EA',
      'JMjv4bLB8Mgl6T',
      'JNaj8oAfG6A1ku',
      'JNalKIHi7dT8Wy',
      'JNall2DcZwYuo1',
      'JNamx36n2DSDiH',
      'JNan03R3ec6vIc',
      'JNap6IzTdpWBXw',
      'JNapsfyStzLm3I',
      'JNaq3BAkIKJDqr',
      'JNarGOzGsEJ44c',
      'JNasiZaivU3Gap',
      'JNauD317XCMwVw',
      'JNax1zcGCfXXp3',
      'JMiyQwl4SI0JBe',
      'JMiyTD5gQEhNe9',
      'JMizSkI6px3897',
      'JMj2fX3ysn7NWG',
      'JMjGQouAJDkBrJ',
      'JMjIh89cnELXnz',
      'JMjvMV5MsK36oM',
      'JMjwdU5iYpotdV',
      'JMjxiII1hgV2eO',
      'JMjxk1uFqgWvzT',
      'JNajGTJQEJo8Om',
      'JNamAun0JWUSmW',
      'JName0Vcc3l5KM',
      'JNaoqskAorD22S',
      'JNapJzXEcfdKIq',
      'JNaqEDNPHdz3Wa',
      'JNb0eyVjolOjBl',
      'JUEkOzLM3qJG1k',
      'JUf41wzIFKAAfK',
      'JMj0uaXG7MGUwd',
      'JMj2VNdNALladk',
      'JMjFGtKjLOmpQb',
      'JMjFNcTmRQmLp2',
      'JMjHAeBY3rzfQN',
      'JMjHk1GBywDAr7',
      'JMjrKNlLfCgT80',
      'JMjsGQPIzwCHV2',
      'JMjsynn1j2iA9I',
      'JMjtEibWFcwk6G',
      'JMjtGojogfOJpz',
      'JMjuHqDsIZEchK',
      'JMjwnqm9VdfUw8',
      'JMjwrGyz3kkLk2',
      'JMjxLQpABZkmfj',
      'JMjzbi2BmY5UdN',
      'JMjztBPGVzsiBw',
      'JNTD4RY6PKhOtL',
      'JNaef6QvZGKqwA',
      'JNahJtxbzI2N94',
      'JNai1bOs5c96xx',
      'JNamKudYd2Z53n',
      'JNan6AhqGaXIxA',
      'JNaqSwddyg6mRh',
      'JNaqrQVoadveId',
      'JNas6U8rhJktY0',
      'JNasT9IykR2hWr',
      'JNasZ2t0QVLwwf',
      'JNatmNik2ARK8W',
      'JNavfTTPbH58rw',
      'JNayqvvLhIMSw7',
      'JNazMwzHPo3eNW',
      'JNb3DQOyiJ7vU6',
      'JMixWJsVCH5m03',
      'JMiy85Ygsncza1',
      'JMjDrW6OgyFbSF',
      'JMjMLTCcD9GExM',
      'JMjYacETszEdvV',
      'JMjqkoZRWZXQ9f',
      'JMjsYzgEPK1YE9',
      'JMjuM1NOfCPCtb',
      'JMjvOvCUsw9bkx',
      'JMjvtIpFZMwYAz',
      'JMjxMECwet7qQY',
      'JMjyBEsrnDsdUI',
      'JNTCvqNWZ0Ik6N',
      'JNakzodTmLBO58',
      'JNalYoRzTVoKUi',
      'JNalnzJHfqby1f',
      'JNalrhxuIw1zlw',
      'JNanFmrZPb4Nix',
      'JNanXLE9ex7ds7',
      'JNasszDEkSvbT5',
      'JNay8lt6970WXl',
      'JUf486hHT23kMd',
      'JMimuhywLjXQnd',
      'JMj5RSPz9G62M5',
      'JMjLdxoiUt2JdB',
      'JMjvBdyhvtpo2g',
      'JMjvtHHqn8WtpD',
      'JMjwH5WeLAeDjn',
      'JMjxuX2Q4n3fpI',
      'JNahP0dd83nywp',
      'JNai1jKpCdEETx',
      'JNam0q1Jux6zFW',
      'JNan82QrETOFHI',
      'JNaoLBrLm2w51H',
      'JNarnTxut4FL7s',
      'JNauPVOyXWFJXr',
      'JNaydPGnFStNwD',
      'JMj1rx8iCcqkDv',
      'JMjM7tT2hcVRBL',
      'JMjsSVPSeAjcSi',
      'JMjtWicfbQ3ASq',
      'JMjvWxfACk3DYA',
      'JMjwIQKNhb0caA',
      'JNafibEcX8SHHe',
      'JNag6GOunnh0s1',
      'JNagp6AO9i0PJ7',
      'JNaiaSlq8Sa5JS',
      'JNajm16fx7y9Pc',
      'JNakM9cdU2P0Df',
      'JNald9xJAGoFCV',
      'JNalvz0y9i08N6',
      'JNapqA7ch7qsPn',
      'JNarA6uHZx7G2D',
      'JNarpAEmDBnJUS',
      'JNau2Y9RmdaiKU',
      'JUklXOLUmOS63u',
      'JMixr3md0sR760',
      'JMixzxcV4EvTva',
      'JMjHFrFeFrK1rm',
      'JMjI6P3tabUlX6',
      'JMjunlH0V3dsxl',
      'JNalJAnafmnKwm',
      'JNam4pnf1yT868',
      'JNamL5nO7LCGfY',
      'JNaoR0Ab2pXBWj',
      'JNaofMhitwSBDg',
      'JNaq1gW8n4PlkO',
      'JNatM2obnOaHnG',
      'JNaw2WrDCM7upP',
      'JNaxBh4gVAMYMg',
      'JNaz1R0Ds5bu8S',
      'JMizUklxTXsR2f',
      'JMj0ZmsQ3KtDP6',
      'JMj4civXD2k75W',
      'JMj503k9XeIwzY',
      'JMjq6mVlzAzw5h',
      'JMjstrQsIkBnhD',
      'JMjt2qZMF5dgiH',
      'JMjurex08O1XWN',
      'JMjv7XQxAHDfdN',
      'JMjx8oNH8qIdgs',
      'JMjxCzeJLkQ8ad',
      'JMjz0xgTv85rF9',
      'JNTDZ8VMrtfXdn',
      'JNamI0gnKq5zex',
      'JNanDrDojxiKre',
      'JNanTJ13xG0xg2',
      'JNanhNfEcQimhw',
      'JNaolC1q5ck8mf',
      'JNaqqM4wnuSFkB',
      'JNawRLBhlaWKn4',
      'JNawzJKxzoDdCr',
      'JMiwl6Xcx779lS',
      'JMixn7FMdblx0g',
      'JMj3oPembAkXMQ',
      'JMj4ZA7416oHb5',
      'JMjEb46i9n7LZG',
      'JMjGB8NWKWN90O',
      'JMjJbC5E0EmMcq',
      'JMjLpnrB2BWiV2',
      'JMjuvbDrSueMzr',
      'JMjxaw073m82iU',
      'JNaekAK6Cl2WEc',
      'JNahFNYeeTEJGy',
      'JNaim8xCBzHnVY',
      'JNakP3qadhTzb2',
      'JNakfuyyt1w4DZ',
      'JNalFhredHgwL1',
      'JNalNnpTe6qo5o',
      'JNalmX3hM7rHwP',
      'JNaokWgSlqTpFV',
      'JNapvkAYHLvFkB',
      'JNarGGbCZhx6VA',
      'JNasF9MxnpQLnW',
      'JNasyeLcmJSBHI',
      'JNayKc9FSLgDtk',
      'JNb1G5TmKlBA3G',
      'JUEk3hIoMcseaF',
      'JMj3dr2RGi0LTc',
      'JMjHuUi0DLPpo4',
      'JMjKrsNibVS02q',
      'JMjq644OfokMT2',
      'JMjtE8dse7SOqk',
      'JMjtvjJkujloRx',
      'JMjvidLf6YTf7I',
      'JMjvmPbUYLyriz',
      'JNakgYjZkvBGG9',
      'JNalu4vKPxHWfu',
      'JNaluGSu9Ni8Q3',
      'JNama01MeaRgmz',
      'JNamvWZ5gRcIsa',
      'JNan7AoXTZOewf',
      'JNanhrABH8dLUw',
      'JNapDuEa0fxU5I',
      'JNaqH50SGb05uq',
      'JNarIzWQvSUjAq',
      'JNatTYZTbYDjVG',
      'JNatdAtoggpJGy',
      'JNatnCy0GAL5dC',
      'JNav42x1eDuRjO',
      'JNaxDcgDcQtPQr',
      'JMj0QLgdGceTRJ',
      'JMj1CFCNBapVtx',
      'JMjDucAHabnFWM',
      'JMjMJsIyodi9cY',
      'JMjvNiQ33wZs6l',
      'JMk0Jq3LPCZ9Jb',
      'JNTD2ndslBE75G',
      'JNajeOnFq0wJMw',
      'JNakXc7i8cNiWq',
      'JNakY3wq5U7kxV',
      'JNakc75Bdg0qMb',
      'JNakqQnqKQtqmN',
      'JNanx2SutkQIJL',
      'JNaqPxrxVh91r4',
      'JNatudrRLpJptp',
      'JNax7sXuMxUXjQ',
      'JNayE8xjdLwvZN',
      'JNazT1M31fpC4V',
      'JOmR7ooW1vQuGG',
      'JMj66FNp8NcuQS',
      'JMjFSj5zSifWu4',
      'JMjFiayczlpQTt',
      'JMjGlWo5xKeGsg',
      'JMjLK9wFiPknFZ',
      'JMjNBVlJ3OOjGY',
      'JMjseRjeTRQcjP',
      'JNakHGs0shW07b',
      'JNakNqtIRcvY2u',
      'JNam5WAVphjc8q',
      'JNam5nkq2X07uS',
      'JNamMAkJKxgXRV',
      'JNan7udHlRWxCl',
      'JNaq2VkWoyeZ51',
      'JNaqTHXHqPXWZO',
      'JNarXjdQx6hahD',
      'JNauJnbprOsJQF',
      'JNax64ZEe1m3Qb',
      'JMj3DPzglcPE51',
      'JMj4M9BiAnaQuD',
      'JMjDHLSUOuSOje',
      'JMjEkjGteNqDyi',
      'JMjKbHjpJ95LVi',
      'JMjMJUfGTKsDTL',
      'JNae2hv3h2IbCn',
      'JNagHmoLeSPrUk',
      'JNahZAf4wsxKip',
      'JNalPAs202Ixza',
      'JNalgqYiI27G1Z',
      'JNalirsdNe5d28',
      'JNamCnJMcpFQok',
      'JNamiYEJuxxycs',
      'JNanpaY96zbpBL',
      'JNapFhOGzVJGCh',
      'JNaqdGdCm9ZnLE',
      'JNaqdslprLlIvT',
      'JNat9yNWUVfcw2',
      'JNavh8yMXGH86b',
      'JNaxu7qYt3qGhm',
      'JNb1GjYtPJSnjY',
      'JMj0Y97Z9XPxXP',
      'JMjuhhaDXWAsNY',
      'JMjwGVVU7gY0f8',
      'JMjxCK6WC89rBe',
      'JMjxpVSaAhI9A5',
      'JNal7cpyiFrkt8',
      'JNamQkHM4B5eEv',
      'JNanB4VWdG4hg0',
      'JNaooQwv8ae4jw',
      'JNapdYDWNbPxeo',
      'JNaqxTmE01e7OW',
      'JNaqxzblSs9OLz',
      'JNarmCaMHw8T8f',
      'JNatGIkKKSnGi1',
      'JNawB1ipZI0qyL',
      'JMixICxI3lkZfS',
      'JMixSziR4WUo27',
      'JMj5Yxgj7gh4SZ',
      'JMjDzgze9BTK0o',
      'JMjFHdEJtj9XnB',
      'JMjuRPjwdU9Lxt',
      'JMjvrUX4O4zCbz',
      'JNaggxY1SjWuWp',
      'JNakaDiWtZ45MP',
      'JNaknLl7QEeBTP',
      'JNamrhIQu9PNDA',
      'JNaodbmTy8q2bC',
      'JNb0bdFNFw2eoH',
      'JNb2oOIB9M2JDl',
      'JMizQZUNjf6wfQ',
      'JMj2N76CcJKABo',
      'JMj3sAZZDpra40',
      'JMjD5z2HIf69IZ',
      'JMjDPOR7nnL9mE',
      'JMjDePeYmeYo4B',
      'JMjKd4MEanRlRF',
      'JMjtO0RR1zQpsm',
      'JMjwbXbnVzXvIf',
      'JNaevPWYs37LUM',
      'JNafK0EcZsv1Xl',
      'JNampmUNGxgGvn',
      'JNamwTgYPrSEhx',
      'JNanFeNfxvJZl0',
      'JNarATLPHWl5dE',
      'JMj03hqXAtWc9f',
      'JMj1J4Vs1E9RJK',
      'JMjFS5eANNPttA',
      'JMjGZu4Bo7sKuC',
      'JMjGr09ofHixpr',
      'JMjqsUFm0j1wVZ',
      'JMjuSDyR8a7PVo',
      'JMjwLOaG0kMG0c',
      'JMjzoVI20pamJq',
      'JMk048KAveWerg',
      'JNah6Gs10CvdEZ',
      'JNakoAasHhzgKs',
      'JNal7297lDz1hc',
      'JNalLfspBg8Wf9',
      'JNamzmyPboSquc',
      'JNankCH2t8YZQW',
      'JNaq3Fj5dBVrNZ',
      'JNat0gMWJ9NrNp',
      'JNauDKznbCm2Xr',
      'JNavy9FKX40XCK',
      'JMj09QgjEY5Bgu',
      'JMj3Hyy06JLweQ',
      'JMjukCEaDr82fP',
      'JMjuu6GoUNuG1w',
      'JMjvBz59izSMXc',
      'JMjwUreuIKnocN',
      'JNTD7HVORVHFjW',
      'JNagdJTAHDVhkU',
      'JNaiIBU958MZhv',
      'JNaiVWQIEiBv8z',
      'JNailQwbSc4aZX',
      'JNajWkVnePatMl',
      'JNakg53qouri45',
      'JNaliBxs6YLhOO',
      'JNaotPCDewvQRq',
      'JNaqOSh693jHor',
      'JNauj4VSjAGoWL',
      'JNavQ4BcgybZ3U',
      'JNawJimNSkzfRT',
      'JNaxVGM2Z30Hzl',
      'JMiy8hdzrj6zkG',
      'JMiyY7Dokz6R6V',
      'JMiyiFimZFpQ64',
      'JMjJ64g2LhvKH6',
      'JMjLYeODGQabtG',
      'JMjpt2c1x4bbP9',
      'JMjr176MJku476',
      'JMjximjrFWybqO',
      'JMjzitNA4DEun9',
      'JMk04jseG2OMmT',
      'JNTDaPeaJbBK92',
      'JNaep5k1So2AqN',
      'JNafD1dNY3HdQN',
      'JNafu1TF0SEtqL',
      'JNal3Saou8Lqkk',
      'JNalDBbU8HoTEB',
      'JNamqZo9GvurGu',
      'JNamyZP2U1L5kH',
      'JNanF4jsMsSwP8',
      'JNao5Nc2XDIajk',
      'JNaoI5oIebVOlF',
      'JNaovFoD0EzUiI',
      'JNapxepBu5OYRg',
      'JNaq0ykCF3iFHb',
      'JNaq14aWGNTmKt',
      'JNaqcVDmG6Qqoz',
      'JNaqvcy9N1cO2f',
      'JNas6R1xID9iSj',
      'JNasVppIphx2BB',
      'JNauQBR8FFrPHq',
      'JNaw6LjwNZyYcg',
      'JNawOHP1PG1aBl',
      'JONvbcEEpvFqC4',
      'JMj4p7Z3HGOfI7',
      'JMjrVpsVSVAYEm',
      'JMjs9gDh0OyHGg',
      'JMjxKVUmu2W1OX',
      'JMjxqsrRR2mSpR',
      'JMjy7VQmNxXr0b',
      'JNai80hEyTw45Z',
      'JNalR38mEe1wnJ',
      'JNan9PoOSbYNeE',
      'JNanSoV4RrrzkZ',
      'JNao0uWL8kx1Lk',
      'JUEkGYFilV7Ek1')

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
'''
query=query.replace('])',')')
query=query.replace('([','(')
sprk_data=sqlContext.sql(query)
pd_df_tokens=sprk_data.toPandas() 


# COMMAND ----------

pd_df_tokens = pd_df_tokens.rename(columns={'id': 'token_id'})

# COMMAND ----------

list_of_tokens

# COMMAND ----------


