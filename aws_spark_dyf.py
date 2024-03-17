
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
# Carregando as tabelas do Glue Catalog
dyf1 = glueContext.create_dynamic_frame.from_catalog(database="db_", table_name="tb_s")
dyf2 = glueContext.create_dynamic_frame.from_catalog(database="db_", table_name="tb_t")
dyf3 = glueContext.create_dynamic_frame.from_catalog(database="db_", table_name="tb_e")

dyf1.show(10)
# Encontrando a data máxima em cada dataframe
max_date_dyf1 = dyf1.toDF().agg({"anomesdia": "max"}).collect()[0][0]
max_date_dyf2 = dyf2.toDF().agg({"data_atualizacao": "max"}).collect()[0][0]

print(max_date_dyf2)
# Filtrando os dados com base na data máxima
dyf1_latest = Filter.apply(frame = dyf1, f = lambda x: x["anomesdia"] == max_date_dyf1)
dyf2_latest = Filter.apply(frame = dyf2, f = lambda x: x["data_atualizacao"] == max_date_dyf2)


dyf2_latest.show(10)
# Join
joined_dyf = dyf1_latest.join(["func"],["func"],dyf2_latest)

joined_dyf.show(10)
# Filtrando os dados da coluna ind_finalizada igual a 1 
joined_dyf_filtered = Filter.apply(frame = joined_dyf, f = lambda x: x["ind_"] == 1)

joined_dyf_filtered.show(5)
# Join para validação
joined_validation = joined_dyf_filtered.join(["func"], ["func"], dyf3)

joined_validation.show(5)
# Filtrar registros onde cod_trilha está contido em cod_module
final_validation = joined_validation.filter(f = lambda x: x["cod_t"] in x["cod_m"])

final_validation.show(5)
# Dropando colunas desnecessárias
#joined_df_filtered = joined_df_filtered.drop_fields(["data_atualizacao", "data_hora_finalizacao"])

dyf_final = final_validation.select_fields(paths=
                                           ["nome_", 
                                            "cod_", 
                                            "ind_", 
                                            "data_hora_finalizacao", 
                                            "func", 
                                            "n_colaborador", 
                                            "e_colaborador", 
                                            "anomesdia"]
                                          )
dyf_final.show(5)
# Gravando arquivo
glueContext.write_dynamic_frame.from_options(
    frame=dyf_final,
    connection_type="s3",
    connection_options={"path": "s3://db_/output/"},
    format="parquet",
    transformation_ctx="datasink2"
)
job.commit()