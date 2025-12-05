# Databricks notebook source
from pyspark.sql import functions as F

catalog = "prime"
schema  = "hospital_management"
volume  = "hm_staff"


# COMMAND ----------

file_path = "/Volumes/prime/hospital_management/hm_staff"

# COMMAND ----------

df_raw= spark.read.csv("/Volumes/prime/hospital_management/hm_staff", header=True, inferSchema=True)

# COMMAND ----------

df_raw.printSchema()

# COMMAND ----------

df_raw.show(5)

# COMMAND ----------

df = (
 df_raw
  .withColumn("staff_id", F.col("staff_id").cast("string")) \
  .withColumn("staff_name", F.col("staff_name").cast("string")) \
  .withColumn("role", F.col("role").cast("string")) \
  .withColumn("service", F.col("service").cast("string"))
)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("prime.hospital_management.hm_staff")


# COMMAND ----------

df = spark.table("prime.hospital_management.hm_staff")
df.show(10)

# COMMAND ----------

df.count()

# COMMAND ----------

--How many staff members are assigned to each service?
df_service_count = (
    df
        .groupBy("service")
        .agg(F.countDistinct("staff_id").alias("staff_count"))
        .orderBy("staff_count", ascending=False)
)

df_service_count.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC --How many staff members are assigned to each service?
# MAGIC SELECT 
# MAGIC   service,
# MAGIC   COUNT(DISTINCT staff_id) AS staff_count
# MAGIC FROM prime.hospital_management.hm_staff
# MAGIC GROUP BY service
# MAGIC ORDER BY staff_count DESC;

# COMMAND ----------

--What is the staff distribution by role within each service?
df_role_distribution = (
    df
        .groupBy("service", "role")
        .agg(F.count("staff_id").alias("total_staff"))
        .orderBy(F.col("service").desc(), F.col("total_staff").desc())
)

df_role_distribution.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC --What is the staff distribution by role within each service?
# MAGIC SELECT 
# MAGIC     service,
# MAGIC     role,
# MAGIC     COUNT(staff_id) AS total_staff
# MAGIC FROM prime.hospital_management.hm_staff
# MAGIC GROUP BY service, role
# MAGIC ORDER BY service DESC, total_staff DESC;