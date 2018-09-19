#!/usr/bin/env python
# coding: utf-8

# In[1]:


# import module

import time
import boto3
import datetime
import cx_Oracle
import pandas as pd
import os
os.environ['LD_LIBRARY_PATH']=':/ORACLE/db/12c/lib'


# In[2]:


# Create an SNS client
client = boto3.client(
    "sns",
    aws_access_key_id="AKIAJAK6LHZ3362CWBWA",
    aws_secret_access_key="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",
    region_name="us-east-1"
)


# In[3]:


# DB connencting

con = cx_Oracle.connect('fimsr/vudrk_read@192.168.1.130:1521/FIMS2005')
con2 = cx_Oracle.connect('11834/3793@192.168.1.127:1521/FUNDDB')
con3 = cx_Oracle.connect('11834/3793@192.168.1.151:1521/IDX01')

cur_fims2005 = con.cursor()
cur_funddb = con2.cursor()
cur_idx01 = con3.cursor()


# In[4]:


# PRINT SQL Result

tablespace_query='SELECT TBS_NM                                     , tbs_sz                                as "Total(MB)"                                    , tbs_alloc_sz                          as "Alloc(MB)"                                    , tbs_alloc_sz - tbs_free_sz            as "Used(MB)"                                    , round((tbs_alloc_sz-tbs_free_sz)/tbs_sz*100,1) AS "Used(%)"                                    , tbs_sz - (tbs_alloc_sz - tbs_free_sz) as "Free(MB)"                                    FROM                                    (                                            SELECT tbs_nm                                            , max(tbs_alloc_sz) as tbs_alloc_sz                                            , max(tbs_sz)       as tbs_sz                                            , max(tbs_free_sz)        as tbs_free_sz                                            FROM                                            (                                                    select tablespace_name                      as tbs_nm                                                    , round(sum(bytes)/power(1024,2),2)    as tbs_alloc_sz                                                    , round(sum(MAXBYTES)/power(1024,2),2) as tbs_sz                                                    , 0                                    As tbs_free_sz                                                    from dba_data_files                                                    group by tablespace_name                                                    union all                                                    select distinct                                                    tablespace_name as tbs_nm                                                    , 0               as tbs_alloc_sz                                                    , 0               as tbs_sz                                                    , round(sum(bytes) over (partition by tablespace_name)/power(1024,2)) As tbs_free_sz                                                    from dba_free_space                                                    )                                            group by tbs_nm                                            ) where 0=0 order by 5 desc'


# In[5]:


# Input Oracle tablespace_amount_used to Pandas DataFrame

fims2005_tablespace  = pd.read_sql(tablespace_query,con,index_col=None)
funddb_tablespace = pd.read_sql(tablespace_query,con2,index_col=None)
idx01_tablespace  = pd.read_sql(tablespace_query,con3,index_col=None)
fims2005_tablespace.head()


# In[6]:


# add DB_NAME to DataFrame
fims2005_tablespace['DB_NAME']='FIMS2005'
funddb_tablespace['DB_NAME']='FUNDDB'
idx01_tablespace['DB_NAME']='IDX01'

# concat each tablespace_DataFrame
all_tablespace = pd.concat([fims2005_tablespace, funddb_tablespace, idx01_tablespace]).reset_index(drop=True)
all_tablespace.head()


# In[13]:


# if amount_used > 95% record Data to list
all_tablespace_result = []
for i in range(len(all_tablespace)):
    if all_tablespace['Used(%)'][i]>94:
        all_tablespace_result.append(all_tablespace.iloc[i])
all_tablespace_result


# In[14]:


# formatting to text
all_tablespace_result2 = []
if len(all_tablespace_result)>=1:
    for i in range(len(all_tablespace_result)):
        all_tablespace_result2.append('{}. {}의 {} 사용률이 {}% 입니다'.format('{}'.format(i+1),all_tablespace_result[i]['DB_NAME'],
                                      all_tablespace_result[i]['TBS_NM'],all_tablespace_result[i]['Used(%)']))
all_tablespace_result3 = []
all_tablespace_result3 = '\n'.join(str(x) for x in all_tablespace_result2)

all_tablespace_result3


# In[15]:


if all_tablespace_result3:
    response = client.publish(
              TopicArn='arn:aws:sns:us-east-1:731292212274:OraErrMsgTopic',
              Message=all_tablespace_result3
              )


# In[ ]:


# DB Closing
cur_fims2005.close()
cur_funddb.close()
cur_idx01.close()

con.close()
con2.close()
con3.close()
