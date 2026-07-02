from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Read CSV Files into DataFrame').getOrCreate()

# Read a single CSV into dataframe using spark.read.csv and then create dataframe with this data using .toPandas().

file_path='Pyspark-Basic\\ReadRRD\\content\\authors.csv'
authors=spark.read.csv(file_path, sep=',',inferSchema=True, header=True)

print("\nRead Single CSV File")
df=authors.toPandas()
#print(df.head())


#D:\\Project for Job\\PySprak\\Pyspark-Basic\\ReadRRD\\content\\authors.csv

path = ['Pyspark-Basic\\ReadRRD\\content\\authors.csv',
        'Pyspark-Basic\\ReadRRD\\content\\book_author.csv']

files = spark.read.csv(path, sep=',',
                       inferSchema=True, header=True)

print(files)
df1 = files.toPandas()
#print(df1)
#print(df1.head())
#print(df1.tail())

print("\nRead all the csv files")

file_path2="Pyspark-Basic/ReadRRD/content/*.csv"

file2 = spark.read.csv(file_path2, sep=',', 
                    inferSchema=True, header=True)

print('\n Read all csv files')

df1 = file2.toPandas()
print(df1.head())
print(df1.tail())


#Pyspark-Basic\ReadRRD\content

