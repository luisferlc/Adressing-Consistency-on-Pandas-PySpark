# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 16:24:44 2024

@author: luisf
"""
### Assertion/Consistency with Polars

import timeit
import polars as pl
import polars.selectors as cs
from polars.testing import assert_frame_equal

tic=timeit.default_timer()

## Reading data and prep for transforms
df_fintech = pl.read_csv("clean_fintech.csv")
df_fintech = df_fintech[['age','credit_score','purchases','zodiac_sign','payment_type','churn','cancelled_loan','received_loan']].clone()
df_fintech = df_fintech.with_columns(pl.col("age").cast(pl.Int64))

# Trasformation functions:
def transform_bool(df):
    for c in df.select(cs.all() - cs.numeric() - cs.string()).columns:
        df = df.with_columns(pl.lit(True).alias(c))
    return df

def transform_str(df):
    for c in df.select(cs.string()).columns:
        df = df.with_columns(pl.col(c).str.replace_all("e",""))
    return df

def transform_numeric(df):
    for c in df.select(cs.numeric()).columns:
        df = df.with_columns(pl.col(c)*2)
    return df

def transform_extracols(df):
    df = df.with_columns([
        (pl.col("purchases").mean()).alias("purchases_mean").cast(pl.Int64),
        (pl.col("age").median()).alias("age_median"),
        (pl.col("credit_score").mean()).alias("score_mean")
    ])
    return df

# Transform and assert function
def assert_transform(df_orig):
    #transform orig
    df_orig = df_orig.pipe(transform_str).pipe(transform_numeric).pipe(transform_bool).pipe(transform_extracols)
    #expected df
    df_expected = pl.DataFrame({
    'age':[42,62,52,66,52],
    'credit_score':[1154.0,1038.0,1085.0312,1116.0,1118.0],
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
    print("Assertion completed succesfully!")

assert_transform(df_fintech)
toc=timeit.default_timer()
print("Polars:", toc - tic)

