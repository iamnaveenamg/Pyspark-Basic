from pyspark.sql import SparkSession
spark=SparkSession.builder.getOrCreate()

from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

# Using PySpark create a DataFrame:
df=spark.createDataFrame([
    Row(a=1,b=2,c='string1',d=date(2000,1,1), e=datetime(2000,1,1,12,0)),
    Row(a=2,b=3,c='string2',d=date(2000,2,1), e=datetime(2000,1,2,12,0)),
    Row(a=4,b=5,c='string3',d=date(2000,3,1), e=datetime(2000,1,3,12,0))

])
print("create a PySpark DataFrame from a list of rows")
print(df)
print(df.show())
df.printSchema()

#Create a PySpark DataFrame with an explicit schema.
df=spark.createDataFrame([
    (1,2,'string1',date(2000,1,1), datetime(2000,1,1,12,0)),
    (2,3,'string2',date(2000,2,1), datetime(2000,1,3,12,0)),
    (3,4,'string1',date(2000,3,1), datetime(2000,1,4,12,0))
],schema='a long, b long,c string, d date, e timestamp')
print("Create a PySpark DataFrame with an explicit schema.")
print(df)
df.printSchema()

# Create a PySpark DataFrame from a pandas DataFrame
pandas_df = pd.DataFrame({
    'a': [1, 2, 3],
    'b': [2., 3., 4.],
    'c': ['string1', 'string2', 'string3'],
    'd': [date(2000, 1, 1), date(2000, 2, 1), date(2000, 3, 1)],
    'e': [datetime(2000, 1, 1, 12, 0), datetime(2000, 1, 2, 12, 0), datetime(2000, 1, 3, 12, 0)]
})
df = spark.createDataFrame(pandas_df)
print("Using Pandas Create PySpark DataFrame")
df
# All DataFrames above result same.
df.show()
df.printSchema()



# Viewing Data
print("Different View Methods:")
df.show(1) # top rows of a dataframe can be displayed using DataFrame.show()

spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
print(df)

df.show(1, vertical=True) #The rows can also be shown vertically. This is useful when rows are too long to show horizontally.
df.columns # DataFrames schema and column names
df.printSchema()
#summary of the DataFrame
df.select("a", "b", "c").describe().show()
df.collect()
df.take(1)
df.toPandas()

## Selecting and Accessing Data
print(df.a)
#most of column-wise operations return Columns.
from pyspark.sql import Column
from pyspark.sql.functions import upper

type(df.c) == type(upper(df.c)) == type(df.c.isNull())
df.select(df.c).show()
df.withColumn('upper_c', upper(df.c)).show() # Assign new column instance
df.filter(df.a == 1).show()
