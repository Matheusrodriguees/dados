# Databricks notebook source
#from pyspark.sql.functions import udf, col, lit
import os
import pandas as pd
import json
from pyspark.sql.functions import *

# COMMAND ----------

MNT_PATH = 'dbfs:/mnt/bigdatadev/'
df = spark.read.format("delta").load(MNT_PATH+'silver/RFB/Estabelecimento/stage/', encoding = 'iso-8859-1')
df = df.withColumnRenamed('COD_CNAE_FISCAL_SECUNDÁRIA', 'COD_CNAE_FISCAL_SECUNDARIA')
#df = df.withColumnRenamed('COD_CNAE_FISCAL_SECUNDÁRIA', 'COD_CNAE_FISCAL_SECUNDARIA') 
display(df)
df.printSchema()
sqlContext.registerDataFrameAsTable(df,'RFB_ESTAB')

# COMMAND ----------

MNT_PATH = 'dbfs:/mnt/bigdatadev/'
df = spark.read.format("delta").load(MNT_PATH+'silver/Dimensoes/DIM_MUN_IBGE_RFB_SINPAS/') 
display(df)
df.printSchema()
sqlContext.registerDataFrameAsTable(df,'DIM_MUN_IBGE_RFB_SINPAS')

# COMMAND ----------

MNT_PATH = 'dbfs:/mnt/bigdatadev/'
df = spark.read.format("delta").load(MNT_PATH+'silver/RFB/Empresas/stage/', enconding = 'iso-8859-1')
#df = df.withColumnRenamed('SALÁRIO', 'SALARIO')  
display(df)
df.printSchema()
sqlContext.registerDataFrameAsTable(df,'RFB_EMPRESA')

# COMMAND ----------

MNT_PATH = 'dbfs:/mnt/bigdatadev/'
df = spark.read.format("delta").load(MNT_PATH+'silver/RFB/Socios/stage/')
#df = df.withColumnRenamed('SALÁRIO', 'SALARIO')  
# display(df)
# df.printSchema()

df = df.groupBy("CNPJ_BASICO").agg(collect_set("NOME_SOCIO").alias("NOME_SOCIOS"),collect_set("CNPJ_CPF_SOCIO").alias("CNPJ_CPF_SOCIOS"),collect_set("COD_IDENTIFICADOR_SOCIO").alias("COD_IDENTIFICADOR_SOCIOS"))


#df.groupBy("CNPJ_BASICO").agg(collect_set("NOME_SOCIO").alias("NOME_SOCIOS")).agg(collect_set("CNPJ_CPF_SOCIO").alias("CNPJ_CPF_SOCIOS")).agg(collect_set("COD_IDENTIFICADOR_SOCIO"#).alias("COD_IDENTIFICADOR_SOCIOS")).show(truncate =False)

# display(df)
# df.printSchema()

df2 = df.withColumn("NOME_SOCIOS", concat_ws(",",col("NOME_SOCIOS"))).withColumn("CNPJ_CPF_SOCIOS", concat_ws(",",col("CNPJ_CPF_SOCIOS"))).withColumn("COD_IDENTIFICADOR_SOCIOS", concat_ws(",",col("COD_IDENTIFICADOR_SOCIOS")))

display(df2)
df2.printSchema()

sqlContext.registerDataFrameAsTable(df2,'RFB_SOCIOS')

# COMMAND ----------

MNT_PATH = 'dbfs:/mnt/bigdatadev/'
df = spark.read.format("delta").load(MNT_PATH+'silver/RFB/Simples/stage/')
#df = df.withColumnRenamed('SALÁRIO', 'SALARIO')  
display(df)
df.printSchema()
sqlContext.registerDataFrameAsTable(df,'RFB_SIMPLES')

# COMMAND ----------

MNT_PATH = 'dbfs:/mnt/bigdatadev/'
df = spark.read.format("delta").load(MNT_PATH+'silver/RFB/Dimensoes/DIM_RFB_MUNICIPIOS/hot/')
#df = df.withColumnRenamed('SALÁRIO', 'SALARIO')  
display(df)
df.printSchema()
sqlContext.registerDataFrameAsTable(df,'RFB_MUN')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS RFB_FATO;
# MAGIC CREATE TABLE RFB_FATO
# MAGIC SELECT
# MAGIC   CONCAT(a.CNPJ_BASICO,a.CNPJ_ORDEM,a.CNPJ_DV) nu_cnpj
# MAGIC   ,a.CNPJ_BASICO AS nu_cnpj_raiz
# MAGIC   ,b.PORTE_DA_EMPRESA as porte_da_empresa
# MAGIC   ,CASE 
# MAGIC     WHEN SUBSTRING(b.NATUREZA_JURIDICA,1,1) = '1' THEN 'GOV'
# MAGIC     WHEN SUBSTRING(b.NATUREZA_JURIDICA,1,1) = '3' THEN 'SFL'
# MAGIC     WHEN b.NATUREZA_JURIDICA = '2135' AND d.OPCAO_MEI = 'S' THEN 'MEI'
# MAGIC     WHEN b.PORTE_DA_EMPRESA in ('1','2') THEN 'Micro'
# MAGIC     WHEN b.PORTE_DA_EMPRESA in ('3','4') THEN 'Pequena'
# MAGIC     WHEN b.PORTE_DA_EMPRESA = '5' AND d.OPCAO_SIMPLES = 'S' THEN 'Pequena'
# MAGIC     WHEN b.PORTE_DA_EMPRESA = '5' AND (d.OPCAO_SIMPLES = 'N' OR d.OPCAO_SIMPLES is null) THEN 'Média e grande'
# MAGIC     ELSE 'Nao informado' 
# MAGIC    END AS nm_porte_obs
# MAGIC   ,CASE
# MAGIC     WHEN b.PORTE_DA_EMPRESA = '0' THEN 'Nao Informado'
# MAGIC     WHEN b.PORTE_DA_EMPRESA = '1' THEN 'Micro Empresa'
# MAGIC     WHEN b.PORTE_DA_EMPRESA = '3' THEN 'Empresa de Pequeno Porte'
# MAGIC     WHEN b.PORTE_DA_EMPRESA = '5' THEN 'Demais' 
# MAGIC     ELSE 'Nao Informado'
# MAGIC    END as nm_porte_receita
# MAGIC   ,a.IDENTIFICADOR AS nm_identificador_filia_matriz
# MAGIC   ,a.NOME_FANTASIA AS nome_fantasia
# MAGIC   ,b.RAZAO_SOCIAL_NOME_EMPRESARIAL AS razao_social
# MAGIC   ,a.SITUACAO_CADASTRAL as situacao_cadastral
# MAGIC   ,a.COD_MOTIVO_SITUACAO_CADASTRAL as  cd_motivo_situacao_cadastral
# MAGIC   ,a.DT_INICIO_ATIVIDADE as dt_inicio_atividadfe
# MAGIC   ,a.COD_CNAE_FISCAL_PRINCIPAL as cd_cnae_fiscal_principal
# MAGIC   ,a.COD_CNAE_FISCAL_SECUNDARIA as cd_cnae_fiscal_secundaria
# MAGIC   ,a.TIPO_DE_LOGRADOURO as tipo_logradouro
# MAGIC   ,a.LOGRADOURO as logradouro
# MAGIC   ,a.NUMERO as numero
# MAGIC   ,a.COMPLEMENTO as complemento
# MAGIC   ,a.BAIRRO as bairro
# MAGIC   ,a.CEP as cep
# MAGIC   ,a.UF as uf
# MAGIC   ,a.MUNICIPIO as cd_mun_rfb
# MAGIC   ,f.DESCRICAO as nm_mun
# MAGIC   ,e.cd7_ibge as cd_mun_ibg
# MAGIC   ,a.DDD_1 as ddd_1
# MAGIC   ,a.TELEFONE_1 as telefone_1
# MAGIC   ,a.DDD_2 as ddd_2
# MAGIC   ,a.TELEFONE_2 as telefone_2
# MAGIC   ,a.CORREIO_ELETRONICO as correio_eletronico
# MAGIC   ,a.DT_SITUACAO_CADASTRAL as dt_situacao_cadastral
# MAGIC   ,d.OPCAO_SIMPLES as opcao_simples
# MAGIC   ,d.OPCAO_MEI as opcao_mei
# MAGIC   ,c.NOME_SOCIOS as nome_socios
# MAGIC   ,c.CNPJ_CPF_SOCIOS as cnpj_cpf_socios
# MAGIC   ,c.COD_IDENTIFICADOR_SOCIOS  as cd_identificador_socios
# MAGIC   
# MAGIC FROM RFB_ESTAB a
# MAGIC LEFT JOIN RFB_EMPRESA b
# MAGIC ON a.CNPJ_BASICO = b.CNPJ_BASICO
# MAGIC LEFT JOIN RFB_SOCIOS c
# MAGIC ON a.CNPJ_BASICO = c.CNPJ_BASICO
# MAGIC LEFT JOIN RFB_SIMPLES d
# MAGIC ON a.CNPJ_BASICO = d.CNPJ_BASICO
# MAGIC LEFT JOIN DIM_MUN_IBGE_RFB_SINPAS e
# MAGIC ON a.MUNICIPIO = e.cd_rfb
# MAGIC LEFT JOIN RFB_MUN f
# MAGIC ON a.MUNICIPIO = f.COD
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC --RFB_ESTAB
# MAGIC
# MAGIC --00000790000296

# COMMAND ----------

dfFato = sqlContext.table("RFB_FATO")


gold_path = 'dbfs:/mnt/bigdatadev/gold/RFB/RFB_FATO/'



gold_path

dfFato.write.mode("overwrite").format("delta").save(gold_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT count(*) FROM RFB_FATO
