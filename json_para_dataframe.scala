// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/Bronze")

// COMMAND ----------

val path = "dbfs:/mnt/dados/Bronze/dataset_imoveis/"
val df = spark.read.format("delta").load(path)

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC Passando json para dataframe

// COMMAND ----------

(df.select("anuncio.*", "anuncio.endereco.*"))

// COMMAND ----------

val dados_detalhados = df.select("anuncio.*", "anuncio.endereco.*")

// COMMAND ----------

val df_silver = dados_detalhados.drop("caracteristicas", "endereco")
display(df_silver)

// COMMAND ----------

val path = "dbfs:/mnt/dados/Silver/dataset_imoveis"
df_silver.write.format("delta").mode("overwrite").save(path)
