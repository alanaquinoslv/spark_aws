
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, split, array_contains
from awsglue.dynamicframe import DynamicFrame


  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
# Carregando as tabelas do Glue Catalog
dyf1 = glueContext.create_dynamic_frame.from_catalog(database="db_", table_name="tb_s")
dyf2 = glueContext.create_dynamic_frame.from_catalog(database="db_", table_name="tb_t")
dyf3 = glueContext.create_dynamic_frame.from_catalog(database="db_", table_name="tb_e")

# Dynamic Frame to Spark DataFrame 
sparkDf1 = dyf1.toDF()
sparkDf2 = dyf2.toDF()
sparkDf3 = dyf3.toDF()

#show spark DF
sparkDf1.show()
# Encontrando a data máxima em cada DataFrame
max_date_df1 = sparkDf1.agg({"anomesdia": "max"}).collect()[0][0]
max_date_df2 = sparkDf2.agg({"data_atualizacao": "max"}).collect()[0][0]

print(max_date_df1)
# Filtrando os dados com base na data máxima
df1_latest = sparkDf1.filter(sparkDf1["anomesdia"] == max_date_df1)
df2_latest = sparkDf2.filter(sparkDf2["data_atualizacao"] == max_date_df2)

df1_latest.show()
# Renomeando coluna p/ evitar ambiguidade
dfRename = df1_latest.withColumnRenamed("func","func_c")
dfRename.show()
# Pegando os que finalizaram da ultima particao
df_ind_1 = dfRename.where("ind_f == 1")
# Fazendo join somente com os que finalizaram
joined_df = df_ind_1.join(df2_latest, df_ind_1.func_c == df2_latest.func, "inner")

joined_df.show()
# Renomeando coluna 
df_e = sparkDf3.withColumnRenamed("anomesdia","mesdiaano")
df_e.show()
# Join p/ validar cod
joined_validation = joined_df.join(df_e, joined_df.func_c == df_e.func, "inner")

joined_validation.show()
# Validando cod |||| usar ~ nega a condicao
final_validation = joined_validation.filter(col("cod_m").contains(col("cod_t")))

final_validation.show(5)
# Dropando colunas desnecessárias
final_df = final_validation.drop(
    "func", 
    "c_cargo", 
    "n_cargo", 
    "data_atualizacao", 
    "pct_",
    "prod", 
    "cat", 
    "cod_m", 
    "ganhador",
    "mesdiaano"
)

final_df.show()
from awsglue.dynamicframe import DynamicFrame

# Convertendo Spark dataframe em Glue dynamicframe
dyf_convert = DynamicFrame.fromDF(final_df, glueContext, "convert")

# Show converted Glue Dynamic Frame
dyf_convert.show()
# Gravando arquivo
glueContext.write_dynamic_frame.from_options(
    frame=dyf_convert,
    connection_type="s3",
    connection_options={"path": "s3://db_/output/"},
    format="parquet",
    transformation_ctx="datasink2"
)
job.commit()