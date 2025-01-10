import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import col, expr,size, array_contains, array_distinct
import pandas as pd
from pyspark.sql.types import array
from pyspark.ml.fpm import FPGrowth

# Inizializzazione della SparkSession
spark = SparkSession.builder.appName("MLQueries").getOrCreate()


def run_kmeans_clustering(df: DataFrame, k: int) -> dict:
    """
    Esegue il KMeans sul dataset e calcola le distanze dai centroidi ai monumenti forniti.
    Restituisce un dizionario con:
    - "labels": lista di liste con latitude, longitude e label assegnata.
    - "centroids": lista di liste con le coordinate dei centroidi.
    - "distanze": lista di distanze tra monumenti e centroidi.
    """
    monuments_path= "Data/monuments.json"
    

    with open(monuments_path, 'r') as f:
        monuments = json.load(f)

    def calculate_distance(lat1, lon1, lat2, lon2):
        import math
        R = 6371  # Raggio terrestre in km
        dLat = math.radians(lat2 - lat1)
        dLon = math.radians(lon2 - lon1)
        a = math.sin(dLat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLon / 2) ** 2
        return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


    df_with_coords = (
        df.withColumn("latitude", col("geoData.latitude"))
          .withColumn("longitude", col("geoData.longitude"))
    )


    filtered_df = df_with_coords.filter(
        col("latitude").isNotNull() &
        col("longitude").isNotNull() &
        (col("latitude").between(-90, 90)) &
        (col("longitude").between(-180, 180))
    )

    # Preparazione delle feature per il KMeans
    assembler = VectorAssembler(
        inputCols=["latitude", "longitude"],
        outputCol="features"
    )
    feature_df = assembler.transform(filtered_df).select("latitude", "longitude", "features")

    # Eseguiamo il KMeans
    kmeans = KMeans().setK(k).setFeaturesCol("features").setPredictionCol("prediction")
    model = kmeans.fit(feature_df)

    # Preparazione delle label
    labeled_coordinates_df = model.transform(feature_df).select("latitude", "longitude", "prediction").distinct()
    labels = labeled_coordinates_df.collect()
    labels_list = [[row.latitude, row.longitude, row.prediction] for row in labels]

    # Preparazione dei centroidi
    centroids = model.clusterCenters()
    centroids_list = [[center[0], center[1]] for center in centroids]

    # Calcolo delle distanze dai monumenti ai centroidi
    distances_list = []
    for i, center in enumerate(centroids):
        distances = []
        for monument in monuments:
            distanza = calculate_distance(
                monument['latitudine'], monument['longitudine'],
                center[0], center[1]
            )
            distances.append({
                "monumento": monument['monumento'],
                "latitudeM": monument['latitudine'],
                "longitudeM": monument['longitudine'],
                "distanza": distanza
            })
        distances_list.append({
            "centroid": {"label": i, "latitude": center[0], "longitude": center[1]},
            "distances": distances
        })

   

    result = {
        "labels": labels_list,
        "centroids": centroids_list,
        "distanze": distances_list
    }

    return result


def calculate_and_filter_association_rules(
    df, 
    min_support=0.2, 
    min_confidence=0.6, 
    target_tags=None
):

    df_filtered = (
    df.filter((col("tags").isNotNull()) & (size(col("tags")) > 0))
      .withColumn("unique_tags_list", array_distinct(col("tags.value")))
    )

    # Prepara i dati per FPGrowth
    transactions_df = df_filtered.select(col("unique_tags_list").alias("items"))

    # Applica FPGrowth
    fpGrowth = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=min_confidence)
    model = fpGrowth.fit(transactions_df)

    # Ottieni le regole di associazione
    association_rules = model.associationRules

    # Filtra le regole di associazione in base ai target_tags
    if target_tags:
        tag_list_lower = [tag.lower() for tag in target_tags]
        for tag in tag_list_lower:
            association_rules = association_rules.filter(array_contains(col("antecedent"), tag))

    return association_rules