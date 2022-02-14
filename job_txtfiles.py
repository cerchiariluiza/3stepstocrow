

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#renderizardata
sourceDyf = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://imobiliario/topics/pagamentos/20220212/"]
    },
    format_options={
        "withHeader": True,
        "separator": ","
    })

sourceDyf.show() 
#acrescentarcamposfaltantes
sourceDyf.show() 
nome_tipo_bonus_relacionamento = {
    
'1' : 'rede', 
'2' : 'boleto', 
'3' : 'fopa', 
'4' : 'opcional', 
'5' : 'opcional_2', 

}



# ler arquivos de pasta normal do windos
newJson.append({
"codigo_cnpj_utilizacao_bonus_relacionamento": f"{1}" ,
"agencia": f"{1}" ,
"conta": f"{1}" ,
"digito": f"{1}" ,
"codigo_tipo_bonus_relacionamento": f"{1}" ,
"valor_bonus_relacionamento": f"{1}" ,
"data_bonus_relacionamento": f"{prefix_date}", 
"nome_tipo_bonus_relacionamento": f"{nome_tipo_bonus_relacionamento['1']}" ,
"identificador_cliente_utilizacao_bonus_relacionamento": "" ,
"codigo_acordo_utilizacao_bonus_relacionamento": f"{'agenciacontadigito'}" ,

}
   )



# Create data frame
sc = SparkContext.getOrCreate('local')
df = spark.read.json(sc.parallelize(newJson), schema, multiLine=True)
df.show()


datasink2 = glueContext.write_dynamic_frame.from_options(frame = sourceDyf, connection_type = "s3", connection_options = {"path": "s3://imobiliario/pastaprincipal/parquet/"}, format = "parquet", transformation_ctx = "datasink2")

job.commit()
