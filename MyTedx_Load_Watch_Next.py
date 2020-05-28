###### TEDx-Load-Aggregate-Model ######

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join
# col serve per accedere alle colonne

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

##### FROM FILES
tedx_dataset_path = "s3://fs-tedx-dati/tedx_dataset.csv"
#il percorso del file che voglio caricare: sono andato in servizi->s3-> ho selezionato il file
#che mi interessa ed esce una finestrella con copia percorso

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()


glueContext = GlueContext(sc)
spark = glueContext.spark_session



job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#### READ INPUT FILES TO CREATE AN INPUT DATASET
tedx_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(tedx_dataset_path)
#la barretta serve per andare a capo e spezzare un comando in più righe
tedx_dataset.printSchema()


#### FILTER ITEMS WITH NULL POSTING KEY
count_items = tedx_dataset.count()
count_items_null = tedx_dataset.filter("idx is not null").count()

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")



## READ TAGS DATASET
tags_dataset_path = "s3://fs-tedx-dati/tags_dataset.csv"
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)

# leggo i watch next (wn)
wn_dataset_path = "s3://fs-tedx-dati/watch_next_dataset.csv"
wn_dataset = spark.read.option("header","true").csv(wn_dataset_path)
wn_dataset = wn_dataset.dropDuplicates()

# CREATE THE AGGREGATE MODEL, ADD TAGS add Watch Next TO TEDX_DATASET e salvo su mongo db
tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags"))
wn_dataset_agg = wn_dataset.select(col("idx"), col("url")).groupBy(col("idx").alias("idx_wn")).agg(collect_list("url").alias("watch_next"))
#aggrego i dati
#collect list va creare un array di tag chiamata tags
tags_dataset_agg.printSchema() #serve a controllare se lo schema è giusto
wn_dataset_agg.printSchema()

tedx_dataset_agg = tedx_dataset.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left") \
    .join(wn_dataset_agg, tedx_dataset.idx == wn_dataset_agg.idx_wn, "left") \
    .drop("idx_ref") \
    .drop("idx_wn") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx") \


tedx_dataset_agg.printSchema()

mongo_uri = "mongodb://mycluster-shard-00-00-wachj.mongodb.net:27017,mycluster-shard-00-01-wachj.mongodb.net:27017,mycluster-shard-00-02-wachj.mongodb.net:27017"

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx",
    "collection": "tedz_wn",
    "username": "myDB",
    "password": "myDBpass",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_agg, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)
