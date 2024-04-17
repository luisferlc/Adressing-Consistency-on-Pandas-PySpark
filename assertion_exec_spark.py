# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 16:38:24 2024

@author: luisf
"""
import timeit
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.pandas as ps
import pandas as pd
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

tic=timeit.default_timer()
# Spark session
spark = SparkSession.builder.appName("consistency_test").getOrCreate()

# Reading and prep data
df_fintech = spark.read.csv("clean_fintech.csv", header = True, inferSchema=True)
df_fintech = df_fintech.select('age','credit_score','purchases','zodiac_sign','payment_type','churn','cancelled_loan','received_loan')
df_fintech = df_fintech.withColumn("age", df_fintech.age.cast("int"))

# Transformation functions
def transform_bool(df):
    for c in [f.name for f in df.schema.fields if isinstance(f.dataType, BooleanType)]:
        df = df.withColumn(c, lit(True))
    return df

def transform_str(df):
    for c in [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]:
        df = df.withColumn(c, regexp_replace(c, 'e', ''))
    return df

def transform_numeric(df):
    for c in [f.name for f in df.schema.fields if isinstance (f.dataType, (IntegerType,DoubleType))]:
        df = df.withColumn(c, df[c]*2)
    return df

def transform_extracols(df):
    df = df.withColumn("purchases_mean", lit(df.select(mean('purchases')).collect()[0][0]))\
             .withColumn("score_mean", lit(df.select(mean('credit_score')).collect()[0][0]))\
            .withColumn("age_median", lit(df.select(median('age')).collect()[0][0]))
    return df

# Transform and assert function
def assert_transform(df_orig):
    #transform orig
    df_orig = df_orig.transform(transform_str).transform(transform_numeric).transform(transform_bool).transform(transform_extracols)
    #expected df
    pandas_df = pd.DataFrame({
    'age':[42,62,52,66,52],
    'credit_score':[1154.0000,1038.0000,1085.031199631591,1116.0000,1118.0000],
    'purchases':[90,0,0,0,0],
    'zodiac_sign':['Piscs','Virgo','Sagittarius','Lo','Virgo'],
    'payment_type':['Smi-Monthly','Bi-Wkly','Wkly','Bi-Wkly','Bi-Wkly'],
    'churn':[True,True,True,True,True],
    'cancelled_loan':[True,True,True,True,True],
    'received_loan':[True,True,True,True,True],
    'purchases_mean':[6.318724749692605,6.318724749692605,6.318724749692605,6.318724749692605,6.318724749692605],
    'score_mean':[1085.1526258518454,1085.1526258518454,1085.1526258518454,1085.1526258518454,1085.1526258518454],
    'age_median':[60.0,60.0,60.0,60.0,60.0]
    })
    ###
    pyspark_schema = StructType([
    StructField('age',IntegerType()),
    StructField('credit_score',DoubleType()),
    StructField('purchases',IntegerType()),
    StructField('zodiac_sign',StringType()),
    StructField('payment_type',StringType()),
    StructField('churn',BooleanType(),False),
    StructField('cancelled_loan',BooleanType(),False),
    StructField('received_loan',BooleanType(),False),
    StructField('purchases_mean',DoubleType(),False),
    StructField('score_mean',DoubleType(), False),
    StructField('age_median',DoubleType(),False)
    ])
    
    df_expected = spark.createDataFrame(pandas_df, pyspark_schema)
    
    assert sorted(df_expected.collect()) == sorted(df_orig.limit(5).collect()), "Assertion failed"
    print("Assertion completed succesfully!")

assert_transform(df_fintech)
toc=timeit.default_timer()
print("Spark:", toc - tic)