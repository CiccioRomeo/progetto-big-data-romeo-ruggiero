from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from flickrapi_utils import fetch_avatar
from pyspark.sql.types import *


# Crea uno Spark UDF per chiamare la funzione fetch_avatar
fetch_avatar_udf = udf(fetch_avatar, StringType())

# Creazione della SparkSession
spark = SparkSession.builder.appName("Fetch Flickr Avatars").getOrCreate()



schema = StructType([
        StructField("camera_info", StructType([
        StructField("make", StringType(), True),
        StructField("model", StringType(), True)
        ]), True),
    StructField("comments", IntegerType(), True),
    StructField("datePosted", StringType(), True),
    StructField("dateTaken", StringType(), True),
    StructField("description", StringType(), True),
    StructField("familyFlag", BooleanType(), True),
    StructField("farm", StringType(), True),
    StructField("favorite", BooleanType(), True),
    StructField("friendFlag", BooleanType(), True),
    StructField("geoData", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("accuracy", IntegerType(), True)
    ]), True),
    StructField("hasPeople", BooleanType(), True),
    StructField("iconFarm", StringType(), True),
    StructField("iconServer", StringType(), True),
    StructField("id", StringType(), True),
    StructField("lastUpdate", StringType(), True),
    StructField("license", StringType(), True),
    StructField("media", StringType(), True),
    StructField("mediaStatus", StringType(), True),
    StructField("notes", ArrayType(StringType()), True),
    StructField("originalFormat", StringType(), True),
    StructField("originalHeight", IntegerType(), True),
    StructField("originalSecret", StringType(), True),
    StructField("originalWidth", IntegerType(), True),
    StructField("owner", StructType([
        StructField("username", StringType(), True),
        StructField("bandwidthUsed", IntegerType(), True),
        StructField("revFamily", BooleanType(), True),
        StructField("photosCount", IntegerType(), True),
        StructField("admin", BooleanType(), True),
        StructField("pro", BooleanType(), True),
        StructField("bandwidthMax", IntegerType(), True),
        StructField("iconServer", IntegerType(), True),
        StructField("revContact", BooleanType(), True),
        StructField("revFriend", BooleanType(), True),
        StructField("id", StringType(), True),
        StructField("filesizeMax", IntegerType(), True),
        StructField("iconFarm", IntegerType(), True)
    ]), True),
    StructField("pathAlias", StringType(), True),
    StructField("photo_url", StringType(), True),
    StructField("placeId", StringType(), True),
    StructField("primary", BooleanType(), True),
    StructField("publicFlag", BooleanType(), True),
    StructField("rotation", IntegerType(), True),
    StructField("secret", StringType(), True),
    StructField("server", StringType(), True),
    StructField("tags", ArrayType(
        StructType([
            StructField("count", IntegerType(), True),
            StructField("value", StringType(), True)
        ])
    ), True),
    StructField("title", StringType(), True),
    StructField("url", StringType(), True),
    StructField("urls", ArrayType(StringType()), True),
    StructField("views", IntegerType(), True)
])

# Percorso del file JSON in input
input_path = "Data/definitivo.json"  # Sostituisci con il percorso del file JSON
output_path = "Data/users_avatar.json"  # Sostituisci con il percorso dell'output

# Legge il dataset JSON
photos_df = spark.read.schema(schema).json(input_path)

# Estrai il campo "owner" per ottenere l'ID utente
owners_df = photos_df.select(col("owner.id").alias("user_id")).distinct()

# Applica la funzione fetch_avatar per ottenere l'URL dell'avatar
avatars_df = owners_df.withColumn("avatar_url", fetch_avatar_udf(col("user_id")))

# Salva l'output in formato JSON
avatars_df.coalesce(1).write.mode("overwrite").json(output_path)

# Mostra i risultati per verifica
avatars_df.show(truncate=False)
