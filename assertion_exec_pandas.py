# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 16:33:58 2024

@author: luisf
"""
### Assertion/Consistency with Pandas

import timeit
import pandas as pd
pd.options.mode.chained_assignment = None 
import numpy as np
from pandas.testing import assert_frame_equal

tic=timeit.default_timer()

# Reading and prep data
df_fintech = pd.read_csv("clean_fintech.csv")
df_fintech = df_fintech[['age','credit_score','purchases','zodiac_sign','payment_type','churn','cancelled_loan','received_loan']].copy()
df_fintech.age = df_fintech.age.astype(np.int64)

### Transformation functions
def transform_bool(df):
    for c in df.select_dtypes(include=['bool']):
        df[c] = True
    return df

def transform_str(df):
    for c in df.select_dtypes(include=['object']):
        df[c] = df[c].str.replace("e","")
    return df

def transform_numeric(df):
    for c in df.select_dtypes(include=['number']):
        df[c] = [i*2 for i in df[c]]
    return df

def transform_extracols(df):
    df['purchases_mean'] = df.purchases.mean().astype(np.int64)
    df['age_median'] = df.age.median()
    df['score_mean'] = df.credit_score.mean()
    
# Transform and assert function

def assert_transform(df_orig):
    #transform orig
    df_orig.pipe(transform_str).pipe(transform_numeric).pipe(transform_bool).pipe(transform_extracols)
    
    #expected df
    df_expected = pd.DataFrame({
    'age':[42,62,52,66,52],
    'credit_score':[1154.0000,1038.0000,1085.0312,1116.0000,1118.0000],
    'purchases':[90,0,0,0,0],
    'zodiac_sign':['Piscs','Virgo','Sagittarius','Lo','Virgo'],
    'payment_type':['Smi-Monthly','Bi-Wkly','Wkly','Bi-Wkly','Bi-Wkly'],
    'churn':[True,True,True,True,True],
    'cancelled_loan':[True,True,True,True,True],
    'received_loan':[True,True,True,True,True],
    'purchases_mean':[6,6,6,6,6],
    'age_median':[60.0,60.0,60.0,60.0,60.0],
    'score_mean':[1085.152626,1085.152626,1085.152626,1085.152626,1085.152626]})
    
    assert_frame_equal(df_expected, df_orig.head()), "Assertion failed"
    print("Assertion completed succesfully")

assert_transform(df_fintech)
toc=timeit.default_timer()
print("Pandas:", toc - tic)