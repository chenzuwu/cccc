import os
import sys
path='/home/job/bigdata-code/'
sys.path.append(path)
###
from common_function.create_sparksession import *
from common_function.send_mail import *
from data_read.dataframe_read_mysql import *
from data_read.spark_read_db import *
from data_clean.row_to_columns import *
from common_function.connect_db import *
from common_function.create_logger import *
from data_clean.mysql_table_struct_to_hive import *
from datetime import datetime,timedelta,date
import time
import pandas as pd
from datetime import datetime,timedelta,date
from extract_transform_load.zhuge_etl.ods.create_zhuge_table import *
from extract_transform_load.zhuge_etl.ods.zhuge_etl import *
now_time =datetime.now()
n=2
conf={"appname":'huanmanbianhua',"driver_memory":'4g',"executor_memory":'8g',"executor_cores":'{0}'.format(n),\
        "executor_num":2}
sc=create_spark(conf)
spark=sc.create_sparksession()


###############获取昨日日期###############

import datetime
def getYesterday(): 
    today=datetime.date.today() 
    oneday=datetime.timedelta(days=1) 
    yesterday=today-oneday  
    return str(yesterday)

###############缓慢变化,数据进行MD5变化，用于数据更新部分###############

import hashlib

def md5_pro(str):
    m = hashlib.md5()
    m.update(str.encode("utf8"))
    return m.hexdigest()

def genMd5(df):
    df['md5_ttt'] = ''
    for i in df.columns:
        df['md5_ttt'] = df['md5_ttt'] + df[i].map(str)
    df['md5_r'] = df['md5_ttt'].apply(md5_pro)
    return df


###############获取两个pandas集合的差集###############

def diff_df(df1,df2,primarycolumn):
    df3 = df1.append(df2)
    df4 = df3.append(df2)
    df_diff_result = df4.drop_duplicates(subset=[primarycolumn],keep=False)
    df_diff_result = df_diff_result[['UNIT_WID','COMPANY_FLAG','CUST_INTERNAL_CODE','BUSINESS_TYPE','DEPARTMENT','CUST_BUSINESS_TYPE','UNIT_CODE','UNIT_NAME','BILLER','CLERK','ZLKH','DISTRICUSTTYPENAME','CHNNEL','LOGIN_DATE']]
    return df_diff_result


###############缓慢变化表更新###############

def scd_process(datestr,datestr_pre,spark):
    spark.sql('''INSERT OVERWRITE TABLE ods.vw_ds_dwzl_fbbc_his
    SELECT * FROM 
    ( 
        SELECT A.unit_wid, 
               A.company_flag, 
               A.cust_internal_code, 
               A.business_type  ,            
               A.department  ,               
               A.cust_business_type  ,       
               A.unit_code  ,                
               A.unit_name  ,                
               A.biller  ,                   
               A.clerk  ,                    
               A.zlkh  ,                     
               A.districusttypename  ,       
               A.chnnel  ,                   
               A.login_date  ,               
               A.t_start_time, 
               CASE 
                    WHEN A.t_end_time = '2099-12-31' AND B.unit_wid IS NOT NULL THEN \'''' + datestr_pre + 
                    '''\' ELSE A.t_end_time 
               END AS t_end_time 
        FROM ods.vw_ds_dwzl_fbbc_his AS A 
        LEFT JOIN ods.vw_ds_dwzl_fbbc_update AS B 
        ON A.unit_wid  = cast(B.unit_wid as int)
    UNION 
        SELECT C.unit_wid,                            
           C.company_flag,                        
           C.cust_internal_code,                  
           C.business_type  ,                     
           C.department  ,                        
           C.cust_business_type  ,                
           C.unit_code  ,                         
           C.unit_name  ,                         
           C.biller  ,                            
           C.clerk  ,                             
           C.zlkh  ,                              
           C.districusttypename  ,                
           C.chnnel  ,                            
           C.login_date  ,                        
                \''''  + datestr + '''\'  AS t_start_time, 
               '2099-12-31' AS t_end_time 
        FROM ods.vw_ds_dwzl_fbbc_update AS C 
    ) AS T '''
)


##############开始FBBC客户表缓慢变化表处理##############

datestr = time.strftime('%Y%m%d',time.localtime(time.time()))
print('''开始处理''' + datestr + '''的数据''')
spark.sql('''create table ods.vw_ds_dwzl_fbbc_''' + datestr + ''' select  * from ods.vw_ds_dwzl_fbbc''')

datestr_pre = getYesterday()[0:4] + getYesterday()[5:7] + getYesterday()[8:10]

df_vw_ds_dwzl_fbbc_today_spark = spark.sql('''select  * from ods.vw_ds_dwzl_fbbc_''' + datestr)
df_vw_ds_dwzl_fbbc_today_pandas = df_vw_ds_dwzl_fbbc_today_spark.toPandas()
df_vw_ds_dwzl_fbbc_today_pandas = df_vw_ds_dwzl_fbbc_today_pandas.drop(columns=['dw_create_time', 'dw_is_deleted'])

df_vw_ds_dwzl_fbbc_pre_spark = spark.sql('''select  * from ods.vw_ds_dwzl_fbbc_''' + datestr_pre)
df_vw_ds_dwzl_fbbc_pre_pandas = df_vw_ds_dwzl_fbbc_pre_spark.toPandas()
df_vw_ds_dwzl_fbbc_pre_pandas = df_vw_ds_dwzl_fbbc_pre_pandas.drop(columns=['dw_create_time', 'dw_is_deleted'])

##############数据新增部分##############
print('''数据新增部分''')
df_vw_ds_dwzl_fbbc_diff = diff_df(df_vw_ds_dwzl_fbbc_today_pandas,df_vw_ds_dwzl_fbbc_pre_pandas,'UNIT_WID')
print('''数据新增''' + str(df_vw_ds_dwzl_fbbc_diff.shape[0]) + '''条''')

##############数据更新部分##############
print('''数据更新部分''')
df_vw_ds_dwzl_fbbc_today_pandas = genMd5(df_vw_ds_dwzl_fbbc_today_pandas)
df_vw_ds_dwzl_fbbc_pre_pandas = genMd5(df_vw_ds_dwzl_fbbc_pre_pandas)

df_vw_ds_dwzl_fbbc_result = pd.merge(df_vw_ds_dwzl_fbbc_today_pandas,df_vw_ds_dwzl_fbbc_pre_pandas,how='inner',on = 'UNIT_WID')
df_vw_ds_dwzl_fbbc_result['is_update'] = df_vw_ds_dwzl_fbbc_result[['md5_r_x', 'md5_r_y']].apply(lambda x: x['md5_r_x'] == x['md5_r_y'], axis=1)

df_vw_ds_dwzl_fbbc_result.rename(columns={'COMPANY_FLAG_x':'COMPANY_FLAG', 'CUST_INTERNAL_CODE_x':'CUST_INTERNAL_CODE', 'BUSINESS_TYPE_x':'BUSINESS_TYPE', 'DEPARTMENT_x':'DEPARTMENT', 'CUST_BUSINESS_TYPE_x':'CUST_BUSINESS_TYPE', 'UNIT_CODE_x':'UNIT_CODE', 'UNIT_NAME_x':'UNIT_NAME', 'BILLER_x':'BILLER', 'CLERK_x':'CLERK', 'ZLKH_x':'ZLKH', 'DISTRICUSTTYPENAME_x':'DISTRICUSTTYPENAME', 'CHNNEL_x':'CHNNEL', 'LOGIN_DATE_x':'LOGIN_DATE'}, inplace=True)
print('''数据更新''' + str(df_vw_ds_dwzl_fbbc_result[df_vw_ds_dwzl_fbbc_result['is_update'] == False].shape[0]) + '''条''')


##############集中新增和数据更新部分##############
print('''集中新增和数据更新部分''')
df_vw_ds_dwzl_fbbc_insertOrUpdate = None
df_vw_ds_dwzl_fbbc_insertOrUpdate = df_vw_ds_dwzl_fbbc_diff 
df_vw_ds_dwzl_fbbc_insertOrUpdate = df_vw_ds_dwzl_fbbc_insertOrUpdate.append(df_vw_ds_dwzl_fbbc_result[df_vw_ds_dwzl_fbbc_result['is_update'] == False][['UNIT_WID','COMPANY_FLAG','CUST_INTERNAL_CODE','BUSINESS_TYPE','DEPARTMENT','CUST_BUSINESS_TYPE','UNIT_CODE','UNIT_NAME','BILLER','CLERK','ZLKH','DISTRICUSTTYPENAME','CHNNEL','LOGIN_DATE']])

schema = StructType([StructField("UNIT_WID", LongType(), True), 
                     StructField("COMPANY_FLAG", StringType(), True), 
                     StructField("CUST_INTERNAL_CODE", StringType(), True), 
                     StructField("BUSINESS_TYPE", StringType(), True), 
                     StructField("DEPARTMENT", StringType(), True), 
                     StructField("CUST_BUSINESS_TYPE", StringType(), True), 
                     StructField("UNIT_CODE", StringType(), True), 
                     StructField("UNIT_NAME", StringType(), True), 
                     StructField("BILLER", StringType(), True), 
                     StructField("CLERK", StringType(), True), 
                     StructField("ZLKH", StringType(), True), 
                     StructField("DISTRICUSTTYPENAME", StringType(), True), 
                     StructField("CHNNEL", StringType(), True), 
                     StructField("LOGIN_DATE", TimestampType(), True)])

df_vw_ds_dwzl_fbbc_insertOrUpdate[['UNIT_WID']] = df_vw_ds_dwzl_fbbc_insertOrUpdate[['UNIT_WID']].astype('int64')
df_1 = spark.createDataFrame(df_vw_ds_dwzl_fbbc_insertOrUpdate,schema)
df_1.write.saveAsTable('ods.vw_ds_dwzl_fbbc_update',mode='overwrite')


##############更新当天的缓慢变化表##############
print('''更新当天的缓慢变化表''')
datestr = datestr[0:4] + '-' +  datestr[4:6]  + '-' + datestr[6:8]
datestr_pre = datestr_pre[0:4] + '-' +  datestr_pre[4:6]  + '-' + datestr_pre[6:8]
scd_process(datestr,datestr_pre,spark)
