# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/data.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ";"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# DBTITLE 1,Limpieza de datos

from pyspark.sql.functions import col, regexp_replace, desc, sum as Fsum, when
from pyspark.sql.types import DecimalType, IntegerType

for colName, colType in df.dtypes: # [ [" Precio Unitario ", "String"], [" Coste Unitario", "String"], ... ]
    df = df.withColumnRenamed(colName, colName.strip())

fields_to_clear = ["Precio Unitario", "Coste unitario", "Importe venta total", "Importe Coste total"]

df2 = df.withColumn("Unidades", col("Unidades").cast(IntegerType()))
for column in fields_to_clear:
    df2 = df2.withColumn(column, regexp_replace(column, " €", "")).withColumn(column, regexp_replace(column, '\.', "")).withColumn(column, regexp_replace(column, ',', '.')).withColumn(column, col(column).cast(DecimalType(20, 3)))

df2.printSchema()
display(df2)

# COMMAND ----------

# DBTITLE 1,Importe de venta total por zonas
sales_by_zone = df2.groupBy("Zona").agg(Fsum("Importe venta total").cast("double").alias("Total"))

display(sales_by_zone)

sales_by_zone.toPandas().plot(kind="bar", x="Zona", figsize=[10, 8], width=0.9, sort_columns=True)

# COMMAND ----------

# DBTITLE 1,Tipo de producto y su canal de venta
#sales_by_channel = df2.groupBy("Tipo de producto", "Canal de venta").agg(when(col("Canal de venta") == "Offline", Fsum("Unidades")).otherwise(0).alias('Offline'), when(col("Canal de venta") == "Online", Fsum("Unidades")).otherwise(0).alias("Online")).sort(col("Tipo de producto"))
sales_by_channel = df2.groupBy("Tipo de producto").pivot("Canal de venta", ["Offline", "Online"]).agg(Fsum("Unidades"))

display(sales_by_channel)

sales_by_channel.toPandas().plot(kind="bar", x="Tipo de producto", figsize=[10, 8], width=0.9, sort_columns=True, stacked=True)

# COMMAND ----------

# DBTITLE 1,Tipo de producto con ventas totales y ganancia total
sales_by_unidades = df2.withColumn("VentasTotales", col("Precio Unitario")* col("Unidades")).withColumn("InversionTotal", col("Coste unitario") * col("Unidades")).withColumn("GananciaTotal", col("VentasTotales") - col("InversionTotal")).groupBy("Tipo de producto").agg(
    Fsum("InversionTotal").cast("double").alias("InversionTotal"), Fsum("VentasTotales").cast("double").alias("VentasTotales"), Fsum("GananciaTotal").cast("double").alias("GananciaTotal")).withColumnRenamed("sum(VentasTotales)", "VentasTotales").withColumnRenamed("sum(InversionTotal)", "InversionTotal").withColumnRenamed("sum(GananciaTotal)", "GananciaTotal")

display(sales_by_unidades)

sales_by_unidades.toPandas().plot(kind="line", x="Tipo de producto", figsize=[10, 8], sort_columns=True, subplots=True)

# COMMAND ----------

# DBTITLE 1,Gráfico del tipo de producto y la cantidad de unidades
sales_by_unidades = df2.withColumn("VentasTotales", col("Precio Unitario")* col("Unidades")).withColumn("InversionTotal", col("Coste unitario") * col("Unidades")).withColumn("GananciaTotal", col("VentasTotales") - col("InversionTotal")).groupBy("Tipo de producto").sum("Unidades", "InversionTotal", "VentasTotales", "GananciaTotal")

display(sales_by_unidades)

sales_by_unidades.toPandas().plot(kind="bar", x="Tipo de producto", figsize=[10, 8], width=0.9, sort_columns=True)

# COMMAND ----------

# DBTITLE 1,Gráfico de ganancias totales de cada producto
sales_by_unidades = df2.withColumn("VentasTotales", col("Precio Unitario")* col("Unidades")).withColumn("InversionTotal", col("Coste unitario") * col("Unidades")).withColumn("GananciaTotal", col("VentasTotales") - col("InversionTotal")).groupBy("Tipo de producto").agg(Fsum("GananciaTotal").cast("double").alias("GananciasTotales"))
#.show()
#sum("GananciaTotal")

display(sales_by_unidades)

sales_by_unidades_pd = sales_by_unidades.toPandas()

sales_by_unidades_pd.plot(kind="pie", y="GananciasTotales", labels=sales_by_unidades_pd["Tipo de producto"], figsize=[10, 10], subplots=True)

# COMMAND ----------

# DBTITLE 1,Ventas totales de cada tipo de producto
sales_by_unidades = df2.withColumn("VentasTotales", col("Precio Unitario")* col("Unidades")).withColumn("InversionTotal", col("Coste unitario") * col("Unidades")).withColumn("GananciaTotal", col("VentasTotales") - col("InversionTotal")).groupBy("Tipo de producto").agg(Fsum("VentasTotales").cast("double").alias("ventasTotales"))
#.show()
#sum("GananciaTotal")

display(sales_by_unidades)

sales_by_unidades.toPandas().plot(kind="bar", x="Tipo de producto", figsize=[10, 8], width=0.9, sort_columns=True)

# COMMAND ----------

# DBTITLE 1,Inversión total por cada producto
sales_by_unidades = df2.withColumn("VentasTotales", col("Precio Unitario")* col("Unidades")).withColumn("InversionTotal", col("Coste unitario") * col("Unidades")).withColumn("GananciaTotal", col("VentasTotales") - col("InversionTotal")).groupBy("Tipo de producto").agg(Fsum("InversionTotal").cast("double").alias("InversionesTotales"))
#.show()
#sum("GananciaTotal")

display(sales_by_unidades)

sales_by_unidades.toPandas().plot(kind="bar", x="Tipo de producto", figsize=[10, 8], width=0.9, sort_columns=True)

# COMMAND ----------

# DBTITLE 1,La prioridad de cada producto
df2.groupBy("Tipo de producto", "Prioridad" ).sum("Unidades").sort(col("Tipo de producto").asc(), col('Prioridad').asc()).show(100)

#display(sales_by_priority)

#sales_by_unidades.toPandas().plot(kind="bar", x="Tipo de producto", figsize=[10, 8], width=0.9, sort_columns=True)

#pd.plot(x="Tipo de producto", figsize=[16,8])

# COMMAND ----------

# DBTITLE 1,Gráfico de la prioridad del producto con la cantidad de unidades 
sales_by_priority = df2.groupBy("Tipo de producto", "Prioridad" ).sum("Unidades").sort(col("Tipo de producto").asc(), col('Prioridad').asc())

display(sales_by_priority)

sales_by_priority.toPandas().plot(kind="bar", x="Prioridad", figsize=[10, 8], width=0.9, sort_columns=True)

# COMMAND ----------

# DBTITLE 1,La igualdad para hacer el gráfico
pd = df2.groupBy("Tipo de producto", "Prioridad" ).sum("Unidades").sort(col("Tipo de producto").asc(), col('Prioridad').asc()).toPandas()

# COMMAND ----------

# DBTITLE 1,Gráfico del tipo de producto con la suma de unidades totales
pd.plot(x="Tipo de producto", figsize=[16,8])
