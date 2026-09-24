import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import from_json, col, to_timestamp

spark = SparkSession.builder.appName("spark_job").getOrCreate()
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "commandes"


#on a que 1 seul broker 1 seul topic et 1 seule partition donc ca dans la doc 
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS ) \
  .option("subscribe", KAFKA_TOPIC) \
  .load()
df_decoded = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


SCHEMA = StructType([
    StructField("commande_id", StringType()),
    StructField("produit", StringType()),
    StructField("prix_unitaire", FloatType()),
    StructField("quantite", IntegerType()),
    StructField("montant_total", FloatType()),
    StructField("ville", StringType()),
    StructField("timestamp", StringType()),
])

#data frame structurer selon les col et et SCHEMA

df_structured = df_decoded.select(
    from_json(col("value"), SCHEMA).alias("data")
)
df_commandes = df_structured.select("data.*")
df_commandes = df_commandes.withColumn("timestamp",to_timestamp(col("timestamp")))
#ecrit dans la console avec le bon format les commandes 

#query = df_commandes \
#    .writeStream \
#    .outputMode("append") \
#    .format("console") \
#    .start()
#query.awaitTermination()



#transferer le data frame dans la table commandes_live dans postgres


def ecrire_dans_postgres(batch_df, batch_id):
    batch_df.write\
    .mode('append')\
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/ecommerce" ) \
    .option("driver", "org.postgresql.Driver")\
    .option("dbtable", "silver.commandes_live") \
    .option("user", os.getenv("POSTGRES_USER")) \
    .option("password", os.getenv("POSTGRES_PASSWORD")) \
    .save()    

query = df_commandes.writeStream \
    .foreachBatch(ecrire_dans_postgres) \
    .start()
query.awaitTermination()