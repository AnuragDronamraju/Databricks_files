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
  and p.created_at < 1725129000
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
  ,md.contact_email
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
and (t.expired_at is null OR cast(from_unixtime(t.expired_at) as date)>CURRENT_DATE)
and t.method in ('emandate','nach')
and term.gateway in ('enach_npci_netbanking','enach_rbl','nach_citi','nach_icici')
and t.created_date < '2024-09-01'
and t.created_date > '2016-04-01'

)


  
SELECT 
  base.merchant_id
  ,base.business_dba
  ,COALESCE(ftt.parent_name,base.business_dba) as parent_name
  ,base.contact_email as Merchant_Email
  ,ftt.name as POC_Name
  ,ftt.owner_email as POC_email
  ,ftt.team_owner
  ,ftt.managed_status
  ,token_id
  ,UMRN_NO
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
LEFT JOIN aggregate_ba.mid_uid_mapping MIUI on base.Merchant_id = MIUI.merchant_id
where Dormancytenure_months >= 6 
and(case when Dormancytenure_months >= 6 AND Dormancytenure_months <= 12 and frequency = 'yearly' then 1 else 0 end) = 0
and managed_status = 'Managed'
and COALESCE(ftt.parent_name,base.business_dba) in
('Safegold [M360]',
'Amica Financial Technologies (Jupiter) Parent [M360]',
'Arthimpact Finserve Private Limited [M360]',
'Sunrise Mentors Private Limited - PA [M360]',
'Ekagrata Finance Private Limited(MyShubhLife) - PA [M360]',
'Credit Wise Capital Private Limited - PA [M360]',
'Wealthy [M360]',
'BENNETT COLEMAN AND CO LTD PA [M360]',
'Western Capital PA [M360]',
'AMPA Orthodontics Pvt Ltd - PA [M360]',
'MyLoanCare Ventures [M360]',
'Piramal Capital & Housing Finance Limited - PA [M360]',
'KodeKloud PA [M360]',
'GLAZE BARTER PVT LTD - PA [M360]',
'Design For Use Consulting Private Limited - PA [M360]',
'BeatoappPA [M360]',
'Kids Clinic India Private Limited - PA [M360]',
'HT DIGITAL STREAMS LTD - PA [M360]',
'WINSPARK INNOVATIONS LEARNING PRIVATE LIMITED - PA [M360]',
'Integrated Enterprises Parent [M360]',
'MEDECINS SANS FRONTIERES INDIA PA [M360]',
'Vistaar Financial services [M360]',
'Value Research (India) Private Limited - PA [M360]',
'PrashantAdvait Foundation - PA [M360]',
'VARTHANA FINANCE PRIVATE LIMITED - PA [M360]',
'Augmont [M360]',
'White Wizard Technologies Private Limited - PA [M360]',
'TVS CREDIT SERVICES LIMITED - PA [M360]',
'Coursera, Inc. Parent [M360]',
'Foundation for Independent Journalism - PA [M360]',
'My LoanCare [M360]',
'Axis Securities Parent [M360]',
'KNAB FINANCE ADVISORS PRIVATE LIMITED [M360]',
'WMG Tech Pvt. Ltd. - PA [M360]',
'Svakarma Finance [M360]',
'KETTO [M360]',
'SBI General Insurance [M360]',
'TRILLIONLOANS FINTECH PRIVATE LIMITED [M360]',
'EQUENTIA FINANCIAL SERVICE PRIVATE LIMITED [M360]',
'CapFront [M360]',
'Cars 24 [M360]',
'KONFLAKE TECH PRIVATE LIMITED - PA [M360]',
'Vested Parent [M360]',
'HDFC Securities [M360]',
'Gromor Finance Private LimitedPA [M360]',
'Transactree Technologies Private Limited [M360]',
'Quicko Infosoft Private Limited - PA [M360]',
'Apport Software Solutions Private LimitedPA [M360]',
'Make A Difference PA [M360]',
'Cigna TTK [M360]',
'Printline Media Private Limited - PA [M360]',
'EZEE TECHNOSYS PRIVATE LIMITED - PA [M360]',
'Save the Children Federation Inc [M360]',
'Wedding Wire PA [M360]',
'Ok Credit [M360]',
'Incred financial services [M360]',
'Travclan Parent [M360]',
'MOHALLA TECH PRIVATE LIMITED [M360]',
'SaveIN Fintech (Parent Account) [M360]',
'SME INTELLECT _ EMandate [M360]',
'Flipkart (Parent Account) [M360]',
'Raising Superstars Enterprises Private Limited - PA [M360]',
'Shree Shivkrupanand Swami Foundation PA [M360]',
'5nance [M360]',
'Royal Bison Autorentals India Pvt Ltd - PA [M360]',
'IE ONLINE MEDIA SERVICES PRIVATE LIMITED - PA [M360]',
'Paisabazaar [M360]',
'KODO [M360]',
'Ananda Vikatan Digital [M360]',
'Rang De P2P Financial Services Pvt Ltd - PA [M360]',
'CRY [M360]',
'ARKA FINCAP LIMITED - PA [M360]',
'Canara HSBC Insurance [M360]',
'WWF-India PA [M360]',
'ISKCON PA [M360]',
'Bhade Pay PA [M360]',
'Shree Krishnayan Desi Gauraksha Avam Golokdham Sewa Samiti PA [M360]',
'Shyam Spectra private limited [M360]',
'UrbanPiper PA [M360]',
'Dezerv Parent Account [M360]',
'Trak N TellPA [M360]',
'Health&Glow [M360]',
'Siddharth Rajsekar Learning Academy [M360]',
'63 Moons Parent Account [M360]',
'Fincfriends [M360]',
'5paisa [M360]',
'XPLANCK MARKETING PRIVATE LIMITED - PA [M360]',
'Impact Guru [M360]',
'BOSCH LIMITED PA [M360]',
'MyOperator PA [M360]',
'IIFL Home Finance [M360]',
'ClinikkPA [M360]',
'Give Foundation [M360]',
'56 AI TECHNOLOGIES PRIVATE LIMITED - PA [M360]',
'GOQII Technologies Private Limited - PA [M360]',
'ScapiaPA [M360]',
'Kenrise Media - PA [M360]',
'LEARNERS GLOBAL GURUKUL LLP - PA [M360]',
'HomeCapital PA [M360]',
'Nexopay [M360]',
'Kovai Media Private Limited - PA [M360]',
'GHV MEDICAL ANCHOR [M360]',
'Mira Money Parent [M360]',
'Talent Sprint [M360]',
'Ultrahuman Parent Account [M360]',
'Onsurity Technologies Private Limited - PA [M360]',
'Art Of Living Group [M360]',
'Gokul Dham Mahatirth PA [M360]',
'AURORAX PRIVATE LIMITED - PA [M360]',
'StockHolding Corporation of India Limited [M360]',
'sRide (Parent Account) [M360]',
'COMIDA TECHNOLOGIES PRIVATE LIMITED - PA [M360]',
'Donatekart Foundation PA [M360]',
'IFL HOUSING FINANCE [M360]',
'News Laundry Media Pvt Ltd - PA [M360]',
'Wryght Research Parent [M360]',
'Gurudev Siddha Peeth PA [M360]',
'Livpure Pvt. Ltd [M360]',
'Cholamandalam GIC [M360]',
'Sponsifyme technologies - PA [M360]',
'Shriram Transport Finance Company Limited [M360]',
'OJAS SOFTECH PRIVATE LIMITED - PA [M360]',
'Testbook [M360]',
'Maitribodh Parivaar Charitable Trust PA [M360]',
'Muthoot Finance Limited PA [M360]',
'REVV [M360]',
'Thaagam Foundation PA [M360]',
'Tricog Health India Private Limited - PA [M360]',
'E2E Networks Limited [M360]',
'Tata AIG Group [M360]',
'ReeLabs Pvt. Ltd - PA [M360]',
'Kotak Life Insurance - Parent [M360]',
'Les Transformations Learning Private Limited - PA [M360]',
'Xcelerating Growth Pvt Ltd - PA [M360]',
'Sulekha.com [M360]',
'HICARE SERVICES PRIVATE LIMITED - PA [M360]',
'Caramelly PA [M360]',
'IntelliTicks PA [M360]',
'BCN Digital [M360]',
'NetworK Kings PA [M360]',
'oneassistPA [M360]',
'Think and Learn Pvt Ltd [M360]',
'REDKENKO HEALTH TECH PRIVATE LIMITED - PA [M360]',
'Sony Liv [M360]',
'Behter Technology Private Limited [M360]',
'ICICI Bank Limited-Parent [M360]',
'Alt Balaji [M360]',
"BENNY'S BOWL PRIVATE LIMITEDPA [M360]",
'ESS PA [M360]',
'Wizklub Learning Private Limited - PA [M360]',
'AERON FLY [M360]',
'Sicilian Training Services LLP - PA [M360]',
'Malayala Manorama [M360]',
'Clapingo PA [M360]',
'Sri Datta Gnana Bodha Sabha Trust (R) PA [M360]',
'GROW MONEY CAPITAL PRIVATE LIMITED - PA [M360]',
'WobbPA [M360]',
'Epowerx Learning Technologies - PA [M360]',
'Tagmango Pvt Ltd - PA [M360]',
'Ola group [M360]',
'Aurogreen Health Private Limited - PA [M360]',
'Zoomcar [M360]',
'KoshexPA [M360]',
'My Great learning [M360]',
'UFABER EDUTECH PRIVATE LIMITED - PA [M360]',
'MJ Digital - PA [M360]',
'ELEPHANTJOB PRIVATE LIMITED - PA [M360]',
'Magicbricks [M360]',
'Strafox Consulting [M360]',
'MARCADONA FASHION MEDIA PRIVATE LIMITED - PA [M360]',
'Lagom Labs Private Limited - PA [M360]',
'Imarticus Learning Pvt Ltd - PA [M360]',
'Ten20 Infomedia Private Limited - PA [M360]',
'Danamojo Online Solutions Private Limited - PA [M360]',
'UPGRAD [M360]',
'Pristyn Care PA [M360]',
'JD Digital PA [M360]',
'Fullstack [M360]',
'trainergoesonlinecom [M360]',
'HOICHOI TECHNOLOGIES [M360]',
'Milaap [M360]',
'CricHeroes Pvt Ltd - PA [M360]'
)

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14

'''

# COMMAND ----------

sprk_data=sqlContext.sql(t1)
pd_df_tokens=sprk_data.toPandas()

# COMMAND ----------

pd_df_tokens.to_csv('/dbfs/FileStore/tables/Anurag/Dormant_MandatesManaged_merchants_3.csv')

# COMMAND ----------

dg = pd.read_csv("/dbfs/FileStore/tables/Anurag/Dormant_MandatesManaged_merchants_3.csv")

# COMMAND ----------

dg.to_csv('/df')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC With debit_data as (
# MAGIC SELECT p.token_id,max(p.created_date) as max_debit_date 
# MAGIC   FROM realtime_hudi_api.payments p
# MAGIC WHERE p.recurring=1
# MAGIC   and p.recurring_type='auto'
# MAGIC   and p.method in ('emandate','nach')
# MAGIC   and p.created_date>='2016-04-01'
# MAGIC   and p.created_at < 1725129000
# MAGIC GROUP BY 1
# MAGIC )
# MAGIC
# MAGIC ,term_data as (
# MAGIC SELECT terminal_id,gateway,gateway_acquirer
# MAGIC   FROM realtime_terminalslive.terminals
# MAGIC   WHERE gateway in ('enach_npci_netbanking','enach_rbl','nach_citi','nach_icici')
# MAGIC   GROUP BY 1,2,3
# MAGIC )
# MAGIC
# MAGIC ,base as (SELECT t.id as token_id
# MAGIC    ,SUBSTR(t.gateway_token,1,20) as UMRN_NO
# MAGIC    ,t.beneficiary_name
# MAGIC    ,t.created_date as MandateCreatedDate
# MAGIC    ,dd.max_debit_date as max_debit_date_date
# MAGIC    ,t.merchant_id as merchant_id
# MAGIC   ,md.business_dba
# MAGIC   ,md.contact_email
# MAGIC   ,t.frequency
# MAGIC   ,CASE WHEN dd.max_debit_date IS NOT NULL THEN DATE_DIFF(month, cast(dd.max_debit_date as date), CAST('2024-08-31' AS date))
# MAGIC   WHEN dd.max_debit_date IS NULL THEN DATE_DIFF(month, cast(t.created_date as date), CAST('2024-08-31' AS date))
# MAGIC   end as Dormancytenure_months 
# MAGIC   ,term.gateway_acquirer
# MAGIC   
# MAGIC FROM realtime_hudi_api.tokens t
# MAGIC LEFT JOIN debit_data dd
# MAGIC   ON dd.token_id=t.id
# MAGIC LEFT JOIN realtime_hudi_api.merchant_details md
# MAGIC   on md.merchant_id=t.merchant_id
# MAGIC INNER JOIN term_data term
# MAGIC     ON term.terminal_id=t.terminal_id
# MAGIC where t.recurring_status='confirmed'
# MAGIC and t.deleted_at is null
# MAGIC and (t.expired_at is null OR cast(from_unixtime(t.expired_at) as date)>CURRENT_DATE)
# MAGIC and t.method in ('emandate','nach')
# MAGIC and term.gateway in ('enach_npci_netbanking','enach_rbl','nach_citi','nach_icici')
# MAGIC and t.created_date < '2024-09-01'
# MAGIC and t.created_date > '2016-04-01'
# MAGIC
# MAGIC )
# MAGIC
# MAGIC
# MAGIC   
# MAGIC SELECT 
# MAGIC   base.merchant_id
# MAGIC   ,MIUI.user_id
# MAGIC   ,base.business_dba
# MAGIC   ,COALESCE(ftt.parent_name,base.business_dba) as parent_name
# MAGIC   ,base.contact_email as Merchant_Email
# MAGIC   ,ftt.name as POC_Name
# MAGIC   ,ftt.owner_email as POC_email
# MAGIC   ,CASE WHEN Dormancytenure_months >= 6 AND Dormancytenure_months <= 12 THEN '6 Months - 1 Year'
# MAGIC   WHEN Dormancytenure_months > 12 AND Dormancytenure_months <= 24  THEN '1 - 2 Years'
# MAGIC   WHEN Dormancytenure_months > 24 AND Dormancytenure_months <= 36  THEN '2 - 3 Years'
# MAGIC   WHEN Dormancytenure_months > 36 AND Dormancytenure_months <= 48  THEN '3 - 4 Years'
# MAGIC   WHEN Dormancytenure_months > 48 AND Dormancytenure_months <= 60  THEN '4 - 5 Years'
# MAGIC   WHEN Dormancytenure_months > 48 AND Dormancytenure_months <= 60  THEN '5 - 6 Years'
# MAGIC   WHEN Dormancytenure_months > 60  THEN '6+ Years'
# MAGIC   END AS Dormancy_Bucket
# MAGIC
# MAGIC
# MAGIC   ,CASE WHEN Dormancytenure_months >= 6 AND Dormancytenure_months <= 12 THEN 1
# MAGIC   WHEN Dormancytenure_months > 12 AND Dormancytenure_months <= 24  THEN 2
# MAGIC   WHEN Dormancytenure_months > 24 AND Dormancytenure_months <= 36  THEN 2
# MAGIC   WHEN Dormancytenure_months > 36 AND Dormancytenure_months <= 48  THEN 3
# MAGIC   WHEN Dormancytenure_months > 48 AND Dormancytenure_months <= 60  THEN 3
# MAGIC   WHEN Dormancytenure_months > 48 AND Dormancytenure_months <= 60  THEN 5
# MAGIC   WHEN Dormancytenure_months > 60  THEN 5
# MAGIC   END AS Penalty_Applicable,
# MAGIC   
# MAGIC   token_id,
# MAGIC
# MAGIC
# MAGIC
# MAGIC FROM base 
# MAGIC LEFT JOIN aggregate_ba.final_team_tagging ftt
# MAGIC   ON ftt.merchant_id=base.merchant_id
# MAGIC where Dormancytenure_months >= 6 
# MAGIC
# MAGIC and(case when Dormancytenure_months >= 6 AND Dormancytenure_months <= 12 and frequency = 'yearly' then 1 else 0 end) = 0
# MAGIC and managed_status = 'Managed'
# MAGIC
# MAGIC and COALESCE(ftt.parent_name,base.business_dba) in
# MAGIC ('Safegold [M360]',
# MAGIC 'Amica Financial Technologies (Jupiter) Parent [M360]',
# MAGIC 'Arthimpact Finserve Private Limited [M360]',
# MAGIC 'Sunrise Mentors Private Limited - PA [M360]',
# MAGIC 'Ekagrata Finance Private Limited(MyShubhLife) - PA [M360]',
# MAGIC 'Credit Wise Capital Private Limited - PA [M360]',
# MAGIC 'Wealthy [M360]',
# MAGIC 'BENNETT COLEMAN AND CO LTD PA [M360]',
# MAGIC 'Western Capital PA [M360]',
# MAGIC 'AMPA Orthodontics Pvt Ltd - PA [M360]',
# MAGIC 'MyLoanCare Ventures [M360]',
# MAGIC 'Piramal Capital & Housing Finance Limited - PA [M360]',
# MAGIC 'KodeKloud PA [M360]',
# MAGIC 'GLAZE BARTER PVT LTD - PA [M360]',
# MAGIC 'Design For Use Consulting Private Limited - PA [M360]',
# MAGIC 'BeatoappPA [M360]',
# MAGIC 'Kids Clinic India Private Limited - PA [M360]',
# MAGIC 'HT DIGITAL STREAMS LTD - PA [M360]',
# MAGIC 'WINSPARK INNOVATIONS LEARNING PRIVATE LIMITED - PA [M360]',
# MAGIC 'Integrated Enterprises Parent [M360]',
# MAGIC 'MEDECINS SANS FRONTIERES INDIA PA [M360]',
# MAGIC 'Vistaar Financial services [M360]',
# MAGIC 'Value Research (India) Private Limited - PA [M360]',
# MAGIC 'PrashantAdvait Foundation - PA [M360]',
# MAGIC 'VARTHANA FINANCE PRIVATE LIMITED - PA [M360]',
# MAGIC 'Augmont [M360]',
# MAGIC 'White Wizard Technologies Private Limited - PA [M360]',
# MAGIC 'TVS CREDIT SERVICES LIMITED - PA [M360]',
# MAGIC 'Coursera, Inc. Parent [M360]',
# MAGIC 'Foundation for Independent Journalism - PA [M360]',
# MAGIC 'My LoanCare [M360]',
# MAGIC 'Axis Securities Parent [M360]',
# MAGIC 'KNAB FINANCE ADVISORS PRIVATE LIMITED [M360]',
# MAGIC 'WMG Tech Pvt. Ltd. - PA [M360]',
# MAGIC 'Svakarma Finance [M360]',
# MAGIC 'KETTO [M360]',
# MAGIC 'SBI General Insurance [M360]',
# MAGIC 'TRILLIONLOANS FINTECH PRIVATE LIMITED [M360]',
# MAGIC 'EQUENTIA FINANCIAL SERVICE PRIVATE LIMITED [M360]',
# MAGIC 'CapFront [M360]',
# MAGIC 'Cars 24 [M360]',
# MAGIC 'KONFLAKE TECH PRIVATE LIMITED - PA [M360]',
# MAGIC 'Vested Parent [M360]',
# MAGIC 'HDFC Securities [M360]',
# MAGIC 'Gromor Finance Private LimitedPA [M360]',
# MAGIC 'Transactree Technologies Private Limited [M360]',
# MAGIC 'Quicko Infosoft Private Limited - PA [M360]',
# MAGIC 'Apport Software Solutions Private LimitedPA [M360]',
# MAGIC 'Make A Difference PA [M360]',
# MAGIC 'Cigna TTK [M360]',
# MAGIC 'Printline Media Private Limited - PA [M360]',
# MAGIC 'EZEE TECHNOSYS PRIVATE LIMITED - PA [M360]',
# MAGIC 'Save the Children Federation Inc [M360]',
# MAGIC 'Wedding Wire PA [M360]',
# MAGIC 'Ok Credit [M360]',
# MAGIC 'Incred financial services [M360]',
# MAGIC 'Travclan Parent [M360]',
# MAGIC 'MOHALLA TECH PRIVATE LIMITED [M360]',
# MAGIC 'SaveIN Fintech (Parent Account) [M360]',
# MAGIC 'SME INTELLECT _ EMandate [M360]',
# MAGIC 'Flipkart (Parent Account) [M360]',
# MAGIC 'Raising Superstars Enterprises Private Limited - PA [M360]',
# MAGIC 'Shree Shivkrupanand Swami Foundation PA [M360]',
# MAGIC '5nance [M360]',
# MAGIC 'Royal Bison Autorentals India Pvt Ltd - PA [M360]',
# MAGIC 'IE ONLINE MEDIA SERVICES PRIVATE LIMITED - PA [M360]',
# MAGIC 'Paisabazaar [M360]',
# MAGIC 'KODO [M360]',
# MAGIC 'Ananda Vikatan Digital [M360]',
# MAGIC 'Rang De P2P Financial Services Pvt Ltd - PA [M360]',
# MAGIC 'CRY [M360]',
# MAGIC 'ARKA FINCAP LIMITED - PA [M360]',
# MAGIC 'Canara HSBC Insurance [M360]',
# MAGIC 'WWF-India PA [M360]',
# MAGIC 'ISKCON PA [M360]',
# MAGIC 'Bhade Pay PA [M360]',
# MAGIC 'Shree Krishnayan Desi Gauraksha Avam Golokdham Sewa Samiti PA [M360]',
# MAGIC 'Shyam Spectra private limited [M360]',
# MAGIC 'UrbanPiper PA [M360]',
# MAGIC 'Dezerv Parent Account [M360]',
# MAGIC 'Trak N TellPA [M360]',
# MAGIC 'Health&Glow [M360]',
# MAGIC 'Siddharth Rajsekar Learning Academy [M360]',
# MAGIC '63 Moons Parent Account [M360]',
# MAGIC 'Fincfriends [M360]',
# MAGIC '5paisa [M360]',
# MAGIC 'XPLANCK MARKETING PRIVATE LIMITED - PA [M360]',
# MAGIC 'Impact Guru [M360]',
# MAGIC 'BOSCH LIMITED PA [M360]',
# MAGIC 'MyOperator PA [M360]',
# MAGIC 'IIFL Home Finance [M360]',
# MAGIC 'ClinikkPA [M360]',
# MAGIC 'Give Foundation [M360]',
# MAGIC '56 AI TECHNOLOGIES PRIVATE LIMITED - PA [M360]',
# MAGIC 'GOQII Technologies Private Limited - PA [M360]',
# MAGIC 'ScapiaPA [M360]',
# MAGIC 'Kenrise Media - PA [M360]',
# MAGIC 'LEARNERS GLOBAL GURUKUL LLP - PA [M360]',
# MAGIC 'HomeCapital PA [M360]',
# MAGIC 'Nexopay [M360]',
# MAGIC 'Kovai Media Private Limited - PA [M360]',
# MAGIC 'GHV MEDICAL ANCHOR [M360]',
# MAGIC 'Mira Money Parent [M360]',
# MAGIC 'Talent Sprint [M360]',
# MAGIC 'Ultrahuman Parent Account [M360]',
# MAGIC 'Onsurity Technologies Private Limited - PA [M360]',
# MAGIC 'Art Of Living Group [M360]',
# MAGIC 'Gokul Dham Mahatirth PA [M360]',
# MAGIC 'AURORAX PRIVATE LIMITED - PA [M360]',
# MAGIC 'StockHolding Corporation of India Limited [M360]',
# MAGIC 'sRide (Parent Account) [M360]',
# MAGIC 'COMIDA TECHNOLOGIES PRIVATE LIMITED - PA [M360]',
# MAGIC 'Donatekart Foundation PA [M360]',
# MAGIC 'IFL HOUSING FINANCE [M360]',
# MAGIC 'News Laundry Media Pvt Ltd - PA [M360]',
# MAGIC 'Wryght Research Parent [M360]',
# MAGIC 'Gurudev Siddha Peeth PA [M360]',
# MAGIC 'Livpure Pvt. Ltd [M360]',
# MAGIC 'Cholamandalam GIC [M360]',
# MAGIC 'Sponsifyme technologies - PA [M360]',
# MAGIC 'Shriram Transport Finance Company Limited [M360]',
# MAGIC 'OJAS SOFTECH PRIVATE LIMITED - PA [M360]',
# MAGIC 'Testbook [M360]',
# MAGIC 'Maitribodh Parivaar Charitable Trust PA [M360]',
# MAGIC 'Muthoot Finance Limited PA [M360]',
# MAGIC 'REVV [M360]',
# MAGIC 'Thaagam Foundation PA [M360]',
# MAGIC 'Tricog Health India Private Limited - PA [M360]',
# MAGIC 'E2E Networks Limited [M360]',
# MAGIC 'Tata AIG Group [M360]',
# MAGIC 'ReeLabs Pvt. Ltd - PA [M360]',
# MAGIC 'Kotak Life Insurance - Parent [M360]',
# MAGIC 'Les Transformations Learning Private Limited - PA [M360]',
# MAGIC 'Xcelerating Growth Pvt Ltd - PA [M360]',
# MAGIC 'Sulekha.com [M360]',
# MAGIC 'HICARE SERVICES PRIVATE LIMITED - PA [M360]',
# MAGIC 'Caramelly PA [M360]',
# MAGIC 'IntelliTicks PA [M360]',
# MAGIC 'BCN Digital [M360]',
# MAGIC 'NetworK Kings PA [M360]',
# MAGIC 'oneassistPA [M360]',
# MAGIC 'Think and Learn Pvt Ltd [M360]',
# MAGIC 'REDKENKO HEALTH TECH PRIVATE LIMITED - PA [M360]',
# MAGIC 'Sony Liv [M360]',
# MAGIC 'Behter Technology Private Limited [M360]',
# MAGIC 'ICICI Bank Limited-Parent [M360]',
# MAGIC 'Alt Balaji [M360]',
# MAGIC "BENNY'S BOWL PRIVATE LIMITEDPA [M360]",
# MAGIC 'ESS PA [M360]',
# MAGIC 'Wizklub Learning Private Limited - PA [M360]',
# MAGIC 'AERON FLY [M360]',
# MAGIC 'Sicilian Training Services LLP - PA [M360]',
# MAGIC 'Malayala Manorama [M360]',
# MAGIC 'Clapingo PA [M360]',
# MAGIC 'Sri Datta Gnana Bodha Sabha Trust (R) PA [M360]',
# MAGIC 'GROW MONEY CAPITAL PRIVATE LIMITED - PA [M360]',
# MAGIC 'WobbPA [M360]',
# MAGIC 'Epowerx Learning Technologies - PA [M360]',
# MAGIC 'Tagmango Pvt Ltd - PA [M360]',
# MAGIC 'Ola group [M360]',
# MAGIC 'Aurogreen Health Private Limited - PA [M360]',
# MAGIC 'Zoomcar [M360]',
# MAGIC 'KoshexPA [M360]',
# MAGIC 'My Great learning [M360]',
# MAGIC 'UFABER EDUTECH PRIVATE LIMITED - PA [M360]',
# MAGIC 'MJ Digital - PA [M360]',
# MAGIC 'ELEPHANTJOB PRIVATE LIMITED - PA [M360]',
# MAGIC 'Magicbricks [M360]',
# MAGIC 'Strafox Consulting [M360]',
# MAGIC 'MARCADONA FASHION MEDIA PRIVATE LIMITED - PA [M360]',
# MAGIC 'Lagom Labs Private Limited - PA [M360]',
# MAGIC 'Imarticus Learning Pvt Ltd - PA [M360]',
# MAGIC 'Ten20 Infomedia Private Limited - PA [M360]',
# MAGIC 'Danamojo Online Solutions Private Limited - PA [M360]',
# MAGIC 'UPGRAD [M360]',
# MAGIC 'Pristyn Care PA [M360]',
# MAGIC 'JD Digital PA [M360]',
# MAGIC 'Fullstack [M360]',
# MAGIC 'trainergoesonlinecom [M360]',
# MAGIC 'HOICHOI TECHNOLOGIES [M360]',
# MAGIC 'Milaap [M360]',
# MAGIC 'CricHeroes Pvt Ltd - PA [M360]'
# MAGIC )
# MAGIC
# MAGIC GROUP BY 1,2,3
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %fs mkdirs /dbfs/FileStore/tables/Anurag/Dormant_Mandates/P0_2

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

# MAGIC %md
# MAGIC
