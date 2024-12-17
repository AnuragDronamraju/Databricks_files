# Databricks notebook source
import pandas as pd

# COMMAND ----------

main_token_list = []

# COMMAND ----------

# BATCH 3


df1 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Edelweiss_Need_to_delete_token_Ids_from_Razorpay_File2.csv")
df2 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Edelweiss_Need_to_delete_token_Ids_from_Razorpay_File1.csv")
df3 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Motilal_Group__M360___1_.csv")
df4 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File1.csv")
df5 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File2.csv")
df6 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File3.csv")
df7 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File4.csv")
df8 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File5.csv")
df9 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File6.csv")
df10 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File7.csv")
df11 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File8.csv")
df12 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File9.csv")
df13 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File10.csv")
df14 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File11.csv")
df15 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File12.csv")
df16 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File13.csv")
df17 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File14.csv")
df18 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File15.csv")
df19 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File16.csv")
df20 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File17.csv")
df21 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File18.csv")
df22 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File19.csv")
df23 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File20.csv")
df24 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File21.csv")
df25 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File22.csv")
df26 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File23.csv")
df27 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File24.csv")
df28 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File25.csv")
df29 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File26.csv")
df30 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File27.csv")
df31 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/smartcoin___Dormant_mandates_delete_File28.csv")
df32 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Grayquest___Tokens__1__xlsx___Sheet1-1.csv")


# COMMAND ----------



# List of Pandas DataFrames
dfs = [df1, df2, df3, df4, df5, df6, df7, df8, df9, df10,
       df11, df12, df13, df14, df15, df16, df17, df18, df19, df20,
       df21, df22, df23, df24, df25, df26, df27, df28, df29, df30,
       df31, df32]

# Concatenate all DataFrames
df_batch3 = pd.concat(dfs, ignore_index=True)



# COMMAND ----------

# BATCH 4

df1 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File1.csv")
df2 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Capital_Float_file_2___Remaining_suspended_revised.csv")
df3 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File2.csv")
df4 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File3.csv")
df5 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File4.csv")
df6 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File5.csv")
df7 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File6.csv")
df8 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File7.csv")
df9 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File8.csv")
df10 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File9.csv")
df11 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File11.csv")
df12 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File10.csv")
df13 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File12.csv")
df14 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File13.csv")
df15 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File14.csv")
df16 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File15.csv")
df17 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File17.csv")
df18 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File16.csv")
df19 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File18.csv")
df20 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File19.csv")
df21 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File20.csv")
df22 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File21.csv")
df23 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File22.csv")
df24 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File23.csv")
df25 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File25.csv")
df26 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File24.csv")
df27 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File26.csv")
df28 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File27.csv")
df29 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File29.csv")
df30 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File28.csv")
df31 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File31.csv")
df32 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File30.csv")
df33 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File33.csv")
df34 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File32.csv")
df35 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File34.csv")
df36 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File35.csv")
df37 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File36.csv")
df38 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File37.csv")
df39 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File38.csv")
df40 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File39.csv")
df41 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File41.csv")
df42 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File40.csv")
df43 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File42.csv")
df44 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File43.csv")
df45 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File45.csv")
df46 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File44.csv")
df47 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File46.csv")
df48 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File47.csv")
df49 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File48.csv")
df50 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File49.csv")
df51 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File1.csv")
df52 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel1_File50.csv")
df53 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File2.csv")
df54 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File3.csv")
df55 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File5.csv")
df56 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File4.csv")
df57 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File6.csv")
df58 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File7.csv")
df59 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File9.csv")
df60 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File8.csv")
df61 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File10.csv")
df62 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File11.csv")
df63 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File13.csv")
df64 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File12.csv")
df65 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File14.csv")
df66 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File15.csv")
df67 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File16.csv")
df68 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File17.csv")
df69 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File18.csv")
df70 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File19.csv")
df71 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File21.csv")
df72 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File20.csv")
df73 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File22.csv")
df74 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File23.csv")
df75 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File24.csv")
df76 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File25.csv")
df77 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File27.csv")
df78 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File26.csv")
df79 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File28.csv")
df80 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File29.csv")
df81 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File30.csv")
df82 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File31.csv")
df83 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File33.csv")
df84 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File32.csv")
df85 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File35.csv")
df86 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File34.csv")
df87 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File36.csv")
df88 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File37.csv")
df89 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File38.csv")
df90 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File39.csv")
df91 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File40.csv")
df92 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File41.csv")
df93 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File41_1_.csv")
df94 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File42.csv")
df95 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File42_1_.csv")
df96 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File43.csv")
df97 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File43_1_.csv")
df98 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File44.csv")
df99 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File44_1_.csv")
df100 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File45.csv")
df101 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File45_1_.csv")
df102 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File46.csv")
df103 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File46_1_.csv")
df104 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File47.csv")
df105 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File47_1_.csv")
df106 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File48.csv")
df107 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File48_1_.csv")
df108 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File49.csv")
df109 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File49_1_.csv")
df110 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File50.csv")
df111 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel2_File50_1_.csv")
df112 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File1.csv")
df113 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File2.csv")
df114 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File3.csv")
df115 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File5.csv")
df116 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File4.csv")
df117 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File6.csv")
df118 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File7.csv")
df119 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File8.csv")
df120 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File9.csv")
df121 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File10.csv")
df122 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File11.csv")
df123 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File12.csv")
df124 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File13.csv")
df125 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File15.csv")
df126 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File14.csv")
df127 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File17.csv")
df128 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File16.csv")
df129 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File18.csv")
df130 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File19.csv")
df131 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File20.csv")
df132 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File21.csv")
df133 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File22.csv")
df134 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File23.csv")
df135 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File24.csv")
df136 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File25.csv")
df137 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File26.csv")
df138 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File27.csv")
df139 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File29.csv")
df140 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/rzp_mandate_cancel3_File28.csv")

# COMMAND ----------

# List of Pandas DataFrames from df1 to df140
dfs = [df1, df2, df3, df4, df5, df6, df7, df8, df9, df10,
       df11, df12, df13, df14, df15, df16, df17, df18, df19, df20,
       df21, df22, df23, df24, df25, df26, df27, df28, df29, df30,
       df31, df32, df33, df34, df35, df36, df37, df38, df39, df40,
       df41, df42, df43, df44, df45, df46, df47, df48, df49, df50,
       df51, df52, df53, df54, df55, df56, df57, df58, df59, df60,
       df61, df62, df63, df64, df65, df66, df67, df68, df69, df70,
       df71, df72, df73, df74, df75, df76, df77, df78, df79, df80,
       df81, df82, df83, df84, df85, df86, df87, df88, df89, df90,
       df91, df92, df93, df94, df95, df96, df97, df98, df99, df100,
       df101, df102, df103, df104, df105, df106, df107, df108, df109, df110,
       df111, df112, df113, df114, df115, df116, df117, df118, df119, df120,
       df121, df122, df123, df124, df125, df126, df127, df128, df129, df130,
       df131, df132, df133, df134, df135, df136, df137, df138, df139, df140]

# Concatenate all 140 DataFrames
df_batch4 = pd.concat(dfs, ignore_index=True)


# COMMAND ----------

# BATCH 5

import pandas as pd

df1 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File1.csv")
df2 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File2.csv")
df3 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File4.csv")
df4 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File3.csv")
df5 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File5.csv")
df6 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File6.csv")
df7 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File8.csv")
df8 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File7.csv")
df9 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File9.csv")
df10 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File10.csv")
df11 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File11.csv")
df12 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File12.csv")
df13 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File14.csv")
df14 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File13.csv")
df15 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File15.csv")
df16 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File16.csv")
df17 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File18.csv")
df18 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File17.csv")
df19 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File20.csv")
df20 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File19.csv")
df21 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File22.csv")
df22 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File21.csv")
df23 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File23.csv")
df24 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File24.csv")
df25 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File25.csv")
df26 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File26.csv")
df27 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File28.csv")
df28 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File27.csv")
df29 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File29.csv")
df30 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File30.csv")
df31 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File31.csv")
df32 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File32.csv")
df33 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File33.csv")
df34 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File34.csv")
df35 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File36.csv")
df36 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File35.csv")
df37 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File37.csv")
df38 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File38.csv")
df39 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File39.csv")
df40 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File40.csv")
df41 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File41.csv")
df42 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File42.csv")
df43 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File43.csv")
df44 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File44.csv")
df45 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File45.csv")
df46 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File46.csv")
df47 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File47.csv")
df48 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File48.csv")
df49 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File49.csv")
df50 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File50.csv")
df51 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File51.csv")
df52 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File52.csv")
df53 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File54.csv")
df54 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File53.csv")
df55 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File55.csv")
df56 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File56.csv")
df57 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File57.csv")
df58 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File58.csv")
df59 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File60.csv")
df60 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File59.csv")
df61 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File61.csv")
df62 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File62.csv")
df63 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File64.csv")
df64 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File63.csv")
df65 = pd.read_csv("/dbfs/FileStore/dbfs/FileStore/tables/Anurag/dormant_mandate_cancellation/Social_Worth_Technologies___Ok_to_cancel_subscription_File65.csv")

# COMMAND ----------

# List of Pandas DataFrames from df1 to df140
dfs = [df1, df2, df3, df4, df5, df6, df7, df8, df9, df10,
       df11, df12, df13, df14, df15, df16, df17, df18, df19, df20,
       df21, df22, df23, df24, df25, df26, df27, df28, df29, df30,
       df31, df32, df33, df34, df35, df36, df37, df38, df39, df40,
       df41, df42, df43, df44, df45, df46, df47, df48, df49, df50,
       df51, df52, df53, df54, df55, df56, df57, df58, df59, df60,
       df61, df62, df63, df64, df65]

# Concatenate all 140 DataFrames
df_batch5 = pd.concat(dfs, ignore_index=True)


# COMMAND ----------

batch3_1 = df_batch3['token_id'].unique().tolist()
batch3_2 = df_batch3['Token ID'].unique().tolist()
batch3_token_list = batch3_1 + batch3_2


# COMMAND ----------

batch4_token_list = df_batch4['token_id'].unique().tolist()
batch5_token_list = df_batch5['token_id'].unique().tolist()


# COMMAND ----------

print(len(batch3_token_list))
print(len(batch4_token_list))
print(len(batch5_token_list))

# COMMAND ----------

main_token_list = list(set(batch3_token_list + batch4_token_list + batch5_token_list))
print(len(main_token_list))


# COMMAND ----------


import math

# Assuming main_token_list is already defined
cleaned_token_list = [token for token in main_token_list if not (isinstance(token, float) and math.isnan(token))]

# Print the cleaned list without NaN values
len(cleaned_token_list)


# COMMAND ----------


# Assuming cleaned_token_list is already defined (with NaN values removed)
final_token_list = [token.replace('token_', '') if isinstance(token, str) else token for token in cleaned_token_list]

# Print the final list with replacements
print(final_token_list)



# COMMAND ----------

#length check
for token in final_token_list:
    if len(token) != 14:
        print(token)


# COMMAND ----------

final_token_list = list(set(final_token_list))
print(len(final_token_list))

# COMMAND ----------


query='''
WITH terminal_data as (
SELECT terminal_id,gateway,merchant_id, gateway_acquirer,get_json_object(identifiers,'$.gateway_merchant_id2') as Utility_Code FROM realtime_terminalslive.terminals
where deleted_at is null 
GROUP BY 1,2,3,4,5
)


select distinct tok.gateway_token as UMRN, term.Utility_Code,
CASE WHEN term.merchant_id in ('100DemoAccount','100000Razorpay') THEN 'Shared' ELSE 'Direct' END as shared_direct,
gateway_acquirer,
tok.merchant_id

FROM realtime_hudi_api.tokens tok
LEFT JOIN terminal_data term ON term.terminal_id=tok.terminal_id
where tok.id in ({})
limit 5000
'''.format(final_token_list)
query=query.replace('])',')')
query=query.replace('([','(')
sprk_data=sqlContext.sql(query)
pd_df=sprk_data.toPandas()

# COMMAND ----------

pd_df.to_csv('/Workspace/Users/dronamraju.anurag@razorpay.com/cancell.csv')

# COMMAND ----------

utility_codes = pd_df['utility code'].unique()

# Create a dictionary to hold DataFrames for each utility code
segregated_dfs = {}

# Iterate over each unique utility code and create a corresponding DataFrame
for code in utility_codes:
    segregated_dfs[code] = pd_df[pd_df['utility code'] == code]
