# coding=utf-8
import pandas as pd
import numpy as np

pd.set_option('max_columns',5)
title = ['time','name','gender','comment','score','replycount']
dtype = ['object','object','int64','object','int64','int64']
df = pd.read_csv('maoyan\\248906_comment20190214164046.txt',delimiter=',',header=None,names=title,quotechar='\"',encoding='utf-8',
                 converters={'time':lambda x:pd.to_datetime(x),
                             'gender':lambda x : pd.to_numeric(x,errors='coerce',downcast='unsigned'),
                             'score': lambda x: pd.to_numeric(x, errors='coerce',downcast='unsigned'),
                             'replycount': lambda x: pd.to_numeric(x, errors='coerce',downcast='unsigned')
                 })
#,dtype=dict(zip(title,dtype)))


print(df.head())
print(df.info())




