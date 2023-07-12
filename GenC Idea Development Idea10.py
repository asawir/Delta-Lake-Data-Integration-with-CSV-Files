# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/shared_uploads/Asawir.Jinabade@cognizant.com/

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/shared_uploads/Asawir.Jinabade@cognizant.com/partialN.csv

# COMMAND ----------

# our path for csv and delta
mycsv_file_path="dbfs:/FileStore/shared_uploads/Asawir.Jinabade@cognizant.com/partialmatch-3.csv"
delta_table_name = "dtable"

# COMMAND ----------

# creating a data frame by name df1
df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(mycsv_file_path)

# COMMAND ----------

# Get the schema of the Delta table
delta_table_schema = spark.table(delta_table_name).schema


# COMMAND ----------

# Get the columns present in the Delta table schema
delta_table_columns = set(delta_table_schema.names)

# COMMAND ----------

delta_table_schema

# COMMAND ----------

# to add Null values we import literal values
from pyspark.sql.functions import lit

# Get the columns present in the DataFrame
df_columns = set(df1.columns)
# remove white spaces
df_columns_stripped = {column.strip() for column in df_columns}

#find matched n unmatched
unmatched_columns = delta_table_columns - df_columns_stripped
matching_columns = delta_table_columns.intersection(df_columns_stripped)

print("unmatched_columns", unmatched_columns)


# here is main logic
# if number of columns in matched and delta columns are equal then write all data
if len(matching_columns) == len(delta_table_columns):
    #df1.createOrReplaceTempView("temp_table")
    df_shuffled = df1.select("rollno", "name", "mobile", "age")
    df_shuffled.createOrReplaceTempView("temp_table")
    spark.sql(f"INSERT INTO {delta_table_name} SELECT * FROM temp_table ")
    print("all data was written to table")
    #spark.sql(f"SELECT * FROM {delta_table_name}").show()
    #if matching columns are less than delta columns add missing columns with null values
elif len(matching_columns) > 0 and len(matching_columns) < len(delta_table_columns):
    for column in unmatched_columns:
        if column not in df_columns:
            df1 = df1.withColumn(column, lit(None).cast("integer"))
            print("column added to view")
    #df1.createOrReplaceTempView("temp_table")
    df_shuffled = df1.select("rollno", "name", "mobile", "age")
    df_shuffled.createOrReplaceTempView("temp_table")
    
    spark.sql(f"INSERT INTO {delta_table_name} SELECT rollno,name,mobile,age FROM temp_table ") 
    print("partial data was written to table")
else:
    # if matchng columns are zero
    print(f"No matching columns in {mycsv_file_path}")
    
print("data in delta table dtable")
spark.sql("select * from dtable").show() 

print("data in temp_table view")
spark.sql("select * from temp_table").show()





# COMMAND ----------

# following will delete our delta table EMPTY THE CATALOG DTABLE remove 2 Hashes
#delete the existing delta table if it is existing
spark.sql("DROP TABLE IF EXISTS default.dtable")
dbutils.fs.rm("dbfs:/user/hive/warehouse/dtable", recurse=True)
#spark.sql("select * from dtable").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dtable
# MAGIC (
# MAGIC   rollno INT,
# MAGIC   name STRING,
# MAGIC   mobile STRING,
# MAGIC   age INT
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------


