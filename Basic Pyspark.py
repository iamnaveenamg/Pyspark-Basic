# Databricks notebook source

from pyspark.sql import SparkSession

# Start Spark
spark=SparkSession.builder.appName("Demo").getOrCreate()

#Sample Data
data = [("Alice",23),("Bob", 30),("Charlie", 19)]

# Create DataFrame
df=spark.createDataFrame(data,["name", "age"])

#show Data
df.show()

#Filter Rows
adults=df.filter(df.age>=21)

print("Adults:")
adults.show()

#Stop Spark
spark.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading - PySpark

# COMMAND ----------

dbutils.fs.ls('/Volumes/workspace/default/big_mart_sales')

# COMMAND ----------

df = spark.read.table('workspace.default.big_mart_sales')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

### DDL Schema
my_ddl_schema='''
                Item_Identifier STRING,
                Item_Weight STRING,
                Item_Fat_Content STRING, 
                Item_Visibility DOUBLE,
                Item_Type STRING,
                Item_MRP DOUBLE,
                Outlet_Identifier STRING,
                Outlet_Establishment_Year INT,
                Outlet_Size STRING,
                Outlet_Location_Type STRING, 
                Outlet_Type STRING,
                Item_Outlet_Sales DOUBLE 

'''

# COMMAND ----------

df = spark.read.table('workspace.default.big_mart_sales') 

# COMMAND ----------

df.display()

# COMMAND ----------

## StruckType Schema
from pyspark.sql.types import * 
from pyspark.sql.functions import *  

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

df.select(col('Item_Identifier'), col('Item_Weight'), col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alias

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_Id')).display()

# COMMAND ----------

### Filter
## s1
df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

### s2
df.filter((col('Item_Type')=='Soft Drinks') & (col('Item_Weight')<10)).display()

# COMMAND ----------

### s3
df.filter((col('Outlet_Size').isNull()) & (col("Outlet_Location_Type").isin('Tier 1', 'Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WithColumn Renamed

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_Wt').display()

# COMMAND ----------

### with column
df=df.withColumn('flag', lit('new'))
df.display()

# COMMAND ----------

df= df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),"Regular", "Reg"))\
    .withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),"Low Fat", "LF"))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Type Casting

# COMMAND ----------

df=df.withColumn('Item_Weight', col('Item_Weight').cast(StringType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Sorting in Pyspark

# COMMAND ----------

## s1
df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()
#s2

# COMMAND ----------

#s3
df.sort(['Item_Weight','Item_Visibility'], ascending=[0,0]).display()

# COMMAND ----------

#s4
df.sort(['Item_Weight', 'Item_Visibility'], ascending=[0,1]).display()
#s5
df.sort(['Item_Weight', 'Item_Visibility'], ascending=[1,0]).display()
#s6


# COMMAND ----------

#### Limit
df.limit(10).display()

# COMMAND ----------

# drop 
df.drop('Item_Visibility').display()

# COMMAND ----------

df.drop('Item_Visibility', 'Item_Type').display()

# COMMAND ----------

# Drop Duplicates
df.dropDuplicates().display()

# COMMAND ----------

df.drop_duplicates(subset=['Item_Type']).display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC Union and Union BY Name

# COMMAND ----------

data1 =[('1','kid'),
       ('2','sid') ]
schema1='id STRING, NAME STRING'
df1=spark.createDataFrame(data1,schema1)

data2=[('3','Rahul'),
       ('4','Jas')]
schema2='id STRING, NAME STRING'
df2=spark.createDataFrame(data2,schema2)


# COMMAND ----------

df1.display()
df2.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

data1= [('kid','1'),
       ('sid','2') ]
schema1='name STRING, id STRING'
df1=spark.createDataFrame(data1,schema1)

df1.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC String Functions

# COMMAND ----------

# InitCap()
df.select(upper('Item_Type').alias('Upper_Item_Name')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Date Functions

# COMMAND ----------

## current Date
df=df.withColumn('curr_date', current_date())
df.display()

# COMMAND ----------

# Date_Add()
df=df.withColumn('week_after', date_add('curr_date',7))

# COMMAND ----------

df.display()

# COMMAND ----------

#date_sub()
df.withColumn('Week_before', date_sub('curr_date',7)).display()

# COMMAND ----------

df=df.withColumn('Week_before', date_sub('curr_date',-7))
df.display()

# COMMAND ----------

#date Diff
df=df.withColumn('datediff',datediff('Week_before', 'curr_date'))
df.display()

# COMMAND ----------

#date format
df=df.withColumn('Week_before', date_format('Week_before','dd-MM-yyyy'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Handling the Null Values

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

df.display()

# COMMAND ----------

#Filling Nulls
df.fillna('NotAvailable').display()

# COMMAND ----------

df.fillna('NotAvailable', subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Split and Indexing

# COMMAND ----------

#split
df.withColumn('OutLet_Type',split('Outlet_Type',' ')).display()

# COMMAND ----------

df.display()

# COMMAND ----------

#indexing
df.withColumn('Outlet__Type',split('Outlet_Type',' ')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Explode

# COMMAND ----------

df_exp=df.withColumn('Outlet_Type',split('Outlet_Type',' '))
df_exp.display()

# COMMAND ----------

df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).display()

# COMMAND ----------

df_exp.withColumn('Type1_falg', array_contains('Outlet_Type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Group By

# COMMAND ----------

#s1
#df.display()
df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

#s2
df.groupBy('Item_Type').agg(avg('Item_MRP')).display()

# COMMAND ----------

#s3
df.groupBy('Item_Type', 'Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP')).display()

# COMMAND ----------

#s4
df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP'), avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Collect List

# COMMAND ----------

data=[
    ('user1','book1'),
    ('user1', 'book2'),
    ('user2', 'book2'),
    ('user2', 'book4'),
    ('user3', 'book1')
]
schema='user String, Book String'
df_book=spark.createDataFrame(data,schema)
df_book.display()

# COMMAND ----------

df_book.groupBy('User').agg(collect_list('book')).display()

# COMMAND ----------

df.select('Item_Type','Outlet_Size','Item_MRP').display()

# COMMAND ----------

#pivot
df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC When Otherwise

# COMMAND ----------

#s1
df=df.withColumn('veg_flag', when(col('Item_Type')=='Meat','Non-Veg').otherwise('veg'))
df.display()

# COMMAND ----------

df.withColumn('veg_exp_flag',when(((col('veg_flag')=='veg') & (col('Item_MRP')<100)), 'veg_Inexpensive')\
    .when((col('veg_flag')=='Veg') & (col('Item_MRP')>100),'veg_Expensive')
    .otherwise('Non_Veg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Joins

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# Inner Join
df1.join(df2,df1['dept_id']==df2['dept_id'],'inner').display()
#left Join
df1.join(df2,df1['dept_id']==df2['dept_id'],'left').display()
#Right Join
df1.join(df2,df1['dept_id']==df2['dept_id'],'right').display()
#Anti Join
df1.join(df2,df1['dept_id']==df2['dept_id'],'anti').display()


# COMMAND ----------

# MAGIC %md
# MAGIC Window Functions

# COMMAND ----------

#Row Number()
#df.display()
from pyspark.sql.window import Window
df.withColumn('rowCol',row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# row vs dense Rank 
df.withColumn('rank',rank().over(Window.orderBy(col('Item_Identifier').desc())))\
    .withColumn('dense_rank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

df.withColumn('dum',sum('Item_MRP').over(Window.orderBy('Item_Identifier').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC User Defined Functions

# COMMAND ----------

#step1
def my_fun(x):
    return x*x;

# COMMAND ----------

#step 2
my_udf=udf(my_fun)

# COMMAND ----------

df.withColumn('mynewcol',my_udf('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Data Writing

# COMMAND ----------

# MAGIC %sql
# MAGIC show VOLUMES IN workspace.default

# COMMAND ----------

#csv
df.write.format('csv')\
    .option('header', True)\
    .mode('overwrite').save('/Volumes/workspace/default/big_mart_sales/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC append

# COMMAND ----------

df.write.format('CSV')\
    .mode('append')\
        .save('/Volumes/workspace/default/big_mart_sales/data.csv')

# COMMAND ----------

df.write.format('csv')\
    .mode('append')\
    .option('path', '/Volumes/workspace/default/big_mart_sales/data.csv')\
    .save()

# COMMAND ----------

#overwrite
df.write.format('CSV')\
    .mode('overwrite')\
        .option('path', '/Volumes/workspace/default/big_mart_sales/data.csv')\
    .save()

# COMMAND ----------

#ignore
df.write.format('csv')\
    .mode('ignore')\
    .option('path', '/Volumes/workspace/default/big_mart_sales/data.csv')\
    .save()

# COMMAND ----------

#parquet
df.write.format('parquet')\
    .mode('overwrite')\
    .option('path', '/Volumes/workspace/default/big_mart_sales/data.parquet')\
    .save()
#

# COMMAND ----------

#table
df.write.format('delta')\
    .mode('overwrite')\
    .saveAsTable('workspace.default.mytable')
#overwrite
df.write.format('parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC Spark SQL

# COMMAND ----------

# Create temp View
df.createTempView('my_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_view where Item_Fat_Content='Lf'

# COMMAND ----------

df_sql=spark.sql("select * from my_view where Item_Fat_Content='Lf'")

# COMMAND ----------

df_sql.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Thank You