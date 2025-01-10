from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, year, month, count, explode, desc, sum, avg, max, size, abs, lit, row_number, array_contains, when,coalesce,
    struct, lower, date_format, hour, count, asc, unix_timestamp, to_timestamp,collect_list,  percentile_approx
)
from pyspark.sql.functions import min as minp


def get_years(df: DataFrame) -> DataFrame:
    """
    Restituisce una lista degli anni univoci basati sul campo 'datePosted'.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con gli anni univoci ordinati.
    """
    return (df.filter(col("datePosted").isNotNull())
              .select(year(col("datePosted")).alias("year"))
              .distinct()
              .orderBy("year"))


def get_first_n_rows(df: DataFrame, n: int) -> DataFrame:
    """
    Restituisce le prime n righe del DataFrame.
    :param df: DataFrame PySpark contenente i dati.
    :param n: Numero di righe da restituire.
    :return: DataFrame con le prime n righe.
    """
    return df.limit(n)

def calculate_views_stats(df) -> DataFrame:
    """
    Calcola la media e la mediana di 'views'.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con media e mediana di 'views'.
    """
    return (df.select(
        avg("views").alias("average_views"),
        percentile_approx("views", 0.5).alias("median_views")
    ))


def count_rows(df : DataFrame):
    """
    Calcola il nuemro di  righe del dataset.
    :param df: DataFrame PySpark contenente il dataset.
    :return: numero righe dataset'.
    """
    return (df.count())


def calculate_comments_stats(df) -> DataFrame:
    """
    Calcola la media e la mediana di 'comments'.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con media e mediana di 'comments'.
    """
    return (df.select(
        avg("comments").alias("average_comments"),
        percentile_approx("comments", 0.5).alias("median_comments")
    ))


def calculate_accuracy_distribution(df) -> DataFrame:
    """
    Calcola la distribuzione dei valori di 'accuracy'.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con la distribuzione dei valori di 'accuracy'.
    """
    return (df.filter(col("geoData.accuracy").isNotNull())
              .groupBy("geoData.accuracy")
              .agg(count("*").alias("count"))
              .orderBy(col("geoData.accuracy").asc()))


def count_user(df: DataFrame)-> DataFrame:
    """
    Restituisce il numero di utenti presenti nel dataset.
    :param df: DataFrame PySpark contenente il dataset.
    :return: Numero totale di utenti unici.
    """
    return(df.select("owner.id").distinct().count())


def calculate_views_by_year(df) -> DataFrame:
    """
    Raggruppa le foto per anno di pubblicazione e calcola la media delle visualizzazioni per anno.
    :param df: DataFrame PySpark contenente il dataset
    :return: DataFrame con anno e media delle visualizzazioni
    """
    return df.groupBy(year(col("datePosted")).alias("yearPosted")).agg(
        avg("views").alias("average_views")
    ).orderBy(col("yearPosted").asc())


def calculate_comments_by_year(df) -> DataFrame:
    """
    Raggruppa le foto per anno di pubblicazione e calcola la media dei commenti per anno.
    :param df: DataFrame PySpark contenente il dataset
    :return: DataFrame con anno e media delle commenti
    """
    return (df.groupBy(year(col("datePosted")).alias("yearPosted"))
              .agg(avg("comments").alias("average_comments"))
              .orderBy(col("yearPosted").asc()))


def count_brand(df: DataFrame):
    """
    Restituisce il numero di brand di fotocamere presenti nel dataset
    :param df: DataFrame PySpark contenente il dataset.
    :return: Numero totale di brand.
    """
    return(df.select("camera_info.make").distinct().count())

# --------HOME--------

def count_photos_by_coordinates(df: DataFrame) -> DataFrame:
    """
    Conta le foto per combinazione di coordinate geografiche (latitudine e longitudine).
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con il conteggio delle foto per coordinate.
    """
    return (df.groupBy(col("geoData.latitude"), col("geoData.longitude"))
              .agg(count("*").alias("photoCount"))
              .orderBy(desc("photoCount")))

# --------ANALYTICS--------

# ~Photo Trends~

def photo_count_by_month_posted(df: DataFrame) -> DataFrame:
    """
    Conta le foto postate per mese.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con il conteggio delle foto postate per mese.
    """
    return (df.filter(col("datePosted").isNotNull())
              .groupBy(month(col("datePosted")).alias("month"))
              .agg(count("*").alias("count"))
              .orderBy("month"))


def photo_count_by_year_posted(df: DataFrame) -> DataFrame:
    """
    Conta le foto postate per anno.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con il conteggio delle foto postate per anno.
    """
    return (df.filter(col("datePosted").isNotNull())
              .groupBy(year(col("datePosted")).alias("year"))
              .agg(count("*").alias("count"))
              .orderBy("year"))


def photo_count_by_month_taken(df: DataFrame) -> DataFrame:
    """
    Conta le foto scattate per mese.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con il conteggio delle foto scattate per mese.
    """
    return (df.filter(col("dateTaken").isNotNull())
              .groupBy(month(col("dateTaken")).alias("month"))
              .agg(count("*").alias("count"))
              .orderBy("month"))


def photo_count_by_year_taken(df: DataFrame) -> DataFrame:
    """
    Conta le foto scattate per anno.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con il conteggio delle foto scattate per anno.
    """
    return (df.filter(col("dateTaken").isNotNull())
              .groupBy(year(col("dateTaken")).alias("year"))
              .agg(count("*").alias("count"))
              .orderBy("year"))


def photo_posted_per_month_by_year_posted(df: DataFrame, input_year: int) -> DataFrame:
    """
    Conta le foto postate per mese in un anno specifico.
    :param df: DataFrame PySpark contenente il dataset.
    :param input_year: Anno da analizzare.
    :return: DataFrame con il conteggio mensile delle foto postate nell'anno specificato.
    """
    return (df.filter((col("datePosted").isNotNull()) & (year(col("datePosted")) == input_year))
              .groupBy(month(col("datePosted")).alias("month"))
              .agg(count("*").alias("count"))
              .orderBy("month"))

def photo_posted_per_month_by_year_taken(df: DataFrame, input_year: int) -> DataFrame:
    """
    Conta le foto scattate per mese in un anno specifico.
    :param df: DataFrame PySpark contenente il dataset.
    :param input_year: Anno da analizzare.
    :return: DataFrame con il conteggio mensile delle foto scattate nell'anno specificato.
    """
    return (df.filter((col("dateTaken").isNotNull()) & (year(col("dateTaken")) == input_year))
              .groupBy(month(col("datePosted")).alias("month"))
              .agg(count("*").alias("count"))
              .orderBy("month"))

def count_photos_posted_per_hour(df) -> DataFrame:
    """
    Conta le foto postate per ora del giorno.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con il conteggio delle foto per ora.
    """
    return (df.withColumn("hourPosted", hour(col("datePosted")))
                .filter(col("hourPosted").isNotNull())
                .groupBy("hourPosted")
                .agg(count("*").alias("count"))
                .orderBy(asc("hourPosted")))


def count_photos_taken_per_hour(df) -> DataFrame:
    """
    Conta le foto scattate per ora del giorno.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con il conteggio delle foto per ora.
    """
    return (df.withColumn("hourTaken", hour(col("dateTaken")))
              .filter(col("hourTaken").isNotNull())
              .groupBy("hourTaken")
              .agg(count("*").alias("photosTakenCount"))
              .orderBy(asc("hourTaken")))


def calculate_average_time_to_post(df) -> DataFrame:
    """
    Calcola il tempo medio (in minuti) tra lo scatto e la pubblicazione delle foto.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con il tempo medio di pubblicazione.
    """
    df_filtered = df.filter((col("datePosted").isNotNull()) & (col("dateTaken").isNotNull()))
    df_with_time_diff = df_filtered.withColumn(
        "timeToPost",
        (unix_timestamp(to_timestamp(col("datePosted"))) - unix_timestamp(to_timestamp(col("dateTaken")))) / 60
    )
    average_time_to_post = df_with_time_diff.agg(avg("timeToPost").alias("averageTimeToPostMinutes"))
    return average_time_to_post

# ~Cameras Analytics~

def top_models_per_year(df: DataFrame) -> DataFrame:
    """
    Restituisce la fotocamera (marchio e modello) più utilizzata per ogni anno con il numero di foto scattate con quella fotocamera.
    
    :param df: DataFrame PySpark contenente i dati delle foto.
    :return: DataFrame con la miglior fotocamera per anno e conteggio foto.
    """

    filtered_df = df.withColumn("make", col("camera_info.make"))\
                    .withColumn("model", col("camera_info.model"))\
                    .filter(
                        (col("make") != "Marca fotocamera non disponibile") &
                        (col("model") != "Modello fotocamera non disponibile")
                    )


    filtered_df = filtered_df.withColumn("year", year(to_timestamp("datePosted")))


    model_year_counts = filtered_df.groupBy("year", "make", "model")\
                                   .agg(count("*").alias("count"))

    # Estra il massimo conteggio per anno
    max_photo_count_per_year = model_year_counts.groupBy("year")\
                                                .agg(max(col("count")).alias("max_count"))\
                                                .select(
                                                    col("year").alias("year_value"),
                                                    col("max_count")
                                                )                                                       

    # Join per andare a estrarre marca e modello per ogni anno 
    model_year_counts = model_year_counts.join(max_photo_count_per_year, model_year_counts["year"] == max_photo_count_per_year["year_value"], "inner")\
                                         .filter(col("count") == col("max_count"))

    # Filtra per il miglior modello di ogni anno
    top_models = model_year_counts.select(
        "year", "make", "model", "count"
    ).orderBy("year")

    return top_models


def top_brands_with_models(df: DataFrame) -> DataFrame:
    """
    Restituisce i top 4 brand e per ogni top brand i 5 modelli più utilizzati in un formato strutturato.
    :param df: DataFrame PySpark contenente i dati delle foto.
    :return: DataFrame con top 5 brand e top 5 modelli per ogni brand in formato nidificato.
    """
    brand_model_counts = df.withColumn("make", col("camera_info.make"))\
                           .withColumn("model", col("camera_info.model"))\
                           .filter(
                                (col("make") != "Marca fotocamera non disponibile") &
                                (col("model") != "Modello fotocamera non disponibile")
                           ).groupBy("make", "model")\
                            . agg(count("*").alias("count"))

    # Trova i top 4 brand
    top_brands = brand_model_counts.groupBy("make")\
                                    .agg(sum("count").alias("total_count"))\
                                    .orderBy(desc("total_count")).limit(4)


    top_brand_models = brand_model_counts.join(top_brands, "make", "inner")


    # Finestra per calcolare la classifica dei modelli all'interno di ogni brand
    window_spec = Window.partitionBy("make").orderBy(desc("count"))
    top_brand_models = top_brand_models.withColumn("rank", row_number().over(window_spec))

    # Filtra per i top 5 modelli di ogni brand
    top_brand_models = top_brand_models.filter(col("rank") <= 5)

    # Raggruppa i modelli in una lista per ogni brand
    result = top_brand_models.groupBy("make","total_count")\
                             .agg(collect_list(struct(col("model").alias("model"),
                                  col("rank").alias("rank"))).alias("models_list"))

    brand_window_spec = Window.orderBy(desc("total_count"))
    result = result.withColumn("rank", row_number().over(brand_window_spec))

    result = result.select(
        col("make").alias("brand"),
        col("rank"),
        col("total_count"),
        col("models_list")
    ).orderBy(col("rank").asc())

    return result

def get_cameras_photo_count_by_brand(df: DataFrame, brand: str) -> DataFrame:
    """
        Filtra e raggruppa le foto per modello di fotocamera in base a un brand specifico.
        :param df: DataFrame PySpark contenente i dati delle foto
        :param brand: Stringa che rappresenta il nome del brand di fotocamere da cercare (case-insensitive).
        :return: DataFrame con due colonne:
                - 'camera_info.model': Il modello della fotocamera.
                - 'count': Il numero totale di foto associate a ciascun modello.
    """
    return (df.filter((lower(col("camera_info.make")).contains(brand.lower())))
               .groupBy("camera_info.model")
               .agg(count("*").alias("count"))
               .orderBy(col("count").desc()))


# ~Tags Analytics~

def get_top_tags(df: DataFrame) -> DataFrame:
    """
    Restituisce i tag più frequenti nel dataset.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con i tag e il loro conteggio.
    """
    return (df.withColumn("tagValue", explode(col("tags.value")))
              .groupBy("tagValue")
              .agg(count("*").alias("count"))
              .orderBy(desc("count")))

# ~Users Analytics~

def top_50_owners(df) -> DataFrame:
    """
    Restituisce i top 50 owner per views totali.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con gli username e il numero di views totali.
    """
    return (df.groupBy("owner.username")
          .agg(sum("views").alias("total_views"))
          .orderBy(col("total_views").desc())
          .limit(50)
    )


def first_post_per_year_month(df) -> DataFrame:
    """
    Calcola il numero di primi post effettuati dagli utenti in ciascun anno e mese.
    :param df: DataFrame PySpark contenente il dataset
    :return: DataFrame con anno, mese e numero di primi post
    """
    
    df_ts = df.withColumn(
        "posted_timestamp",
        to_timestamp("datePosted", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    )

    first_posts_df = (
        df_ts.groupBy("owner.id")
             .agg(minp("posted_timestamp").alias("first_post_ts"))
    )

    first_posts_df = first_posts_df.withColumn("year", year("first_post_ts")) \
                                   .withColumn("month", month("first_post_ts"))

    # Raggruppiamo per anno e mese, contando il numero di utenti 
    # che hanno il loro "primo post" in quell'anno/mese
    result = (
        first_posts_df.groupBy("year", "month")
                      .agg(count("*").alias("count"))
                      .orderBy("year", "month")
    )
    return result


def calculate_pro_user_distribution(df) -> DataFrame:
    """
    Conta il numero di utenti pro e non-pro.
    :param df: DataFrame PySpark contenente il dataset
    :return: DataFrame con il conteggio di utenti pro e non-pro
    """
    return (df.filter(col("owner.pro").isNotNull())
             .groupBy("owner.pro")
             .agg(count("*").alias("user_count"))   
             .orderBy(col("owner.pro").desc()))



def search_owner_with_cameras(df: DataFrame, avatar_df: DataFrame, username: str) -> DataFrame:
    """
    Cerca informazioni sull'utente basandosi sul nome utente, inclusi i dettagli delle fotocamere utilizzate.
    Esclude righe senza informazioni valide sulla marca e sul modello della fotocamera solo nella fase finale.
    
    :param df: DataFrame PySpark contenente i dati.
    :param avatar_df: DataFrame PySpark contenente i dati degli avatar degli utenti.
    :param username: Nome utente da cercare.
    :return: DataFrame con informazioni dettagliate sull'utente e le fotocamere utilizzate.
    """
 
    df = (df
          .withColumn("owner_id", col("owner.id"))
          .withColumn("owner_username", col("owner.username"))
          .withColumn("camera_make", col("camera_info.make"))
          .withColumn("camera_model", col("camera_info.model"))
          .withColumn("photo_views", col("views").cast("int"))
          .withColumn("photo_comments", col("comments").cast("int")))

    user_summary_df = (df
        .groupBy("owner_id", "owner_username")
        .agg(
            sum(col("photo_views")).alias("total_views"),
            sum(col("photo_comments")).alias("total_comments"),
            count("id").alias("total_photos"),
            max(struct(
                col("photo_views").alias("views"),
                col("photo_comments").alias("comments"),
                col("photo_url").alias("url")
            )).alias("max_photo")
        )
        .withColumn("rank", row_number().over(Window.orderBy(col("total_views").desc())))
        .select(
            col("rank"),
            col("owner_id"),
            col("owner_username").alias("username"),
            col("total_photos"),
            col("total_comments"),
            col("max_photo.views").alias("most_viewed_photo_views"),
            col("max_photo.comments").alias("most_viewed_photo_comments"),
            col("max_photo.url").alias("best_photo_url"),
            col("total_views")
        )
    )

    #Filtrare per nome utente (se specificato)
    if username:
        user_summary_df = user_summary_df.filter(lower(col("username")).contains(username.lower()))

  
    user_summary_df = user_summary_df.join(avatar_df, user_summary_df["owner_id"] == avatar_df["user_id"], "inner")


    filtered_user_ids = user_summary_df.select("user_id").distinct()

    camera_df = (df
        .join(filtered_user_ids, df["owner_id"] == filtered_user_ids["user_id"], "inner")
        .filter((col("camera_make") != "Marca fotocamera non disponibile") &
                (col("camera_model") != "Modello fotocamera non disponibile"))
        .groupBy("owner_id", "camera_make", "camera_model")
        .agg(count("id").alias("photo_count"))
        .groupBy("owner_id")
        .agg(
            collect_list(struct(
                col("camera_make"),
                col("camera_model"),
                col("photo_count")
            )).alias("camera_details")
        )
        .withColumn("user_id", col("owner_id")).drop("owner_id")
    )


    result_df = user_summary_df.join(camera_df, ["user_id"], "left").orderBy(col("rank").asc())

    return result_df

# --------SEARCH PHOTOS--------

def search_photos(df: DataFrame, keyword=None, dataInizio=None, dataFine=None, tag_list=None) -> DataFrame:
    """
        Restituisce un dataframe con le foto filtrate in base ai parametri passati
        :param df: DataFrame PySpark contenente il dataset.
        :param keyword: parola chiave cercata in descrzione nome utente titolo e tag
        :param dataInizio: utilizzata per effettuare ricerche per range di date
        :param dataInizio: utilizzata per effettuare ricerche per range di date
        :tag_list: lista di tag per effettuare ricerca per tag
        :return: DataFrame 
    """
 
    filtered_df = df.withColumn("tags_array", col("tags.value"))

    if keyword:
        keyword_lower = keyword.lower()
    
        
        filtered_df = filtered_df.filter(
            (lower(col("title")).contains(keyword_lower)) |
            (lower(col("description")).contains(keyword_lower)) |
            (lower(col("owner.username")).contains(keyword_lower))
        )

    
    if dataInizio or dataFine:
        if dataInizio and not dataFine:
            filtered_df = filtered_df.filter(col("datePosted") >= lit(dataInizio))
        elif dataFine and not dataInizio:
            filtered_df = filtered_df.filter(col("datePosted") <= lit(dataFine))
        elif dataInizio and dataFine:
            filtered_df = filtered_df.filter(
                (col("datePosted") >= lit(dataInizio)) & (col("datePosted") <= lit(dataFine))
            )

    
    if tag_list:
        tag_list_lower = [tag.lower() for tag in tag_list]

       
        for tag in tag_list_lower:
            filtered_df = filtered_df.filter(
                array_contains(col("tags_array"), tag) 
            )


    # Formatta le colonne dateTaken e datePosted (Frontend)     
    formatted_df = filtered_df.withColumn(
        "dateTakenFormatted",
        when(col("dateTaken").isNull(), "N/A").otherwise(date_format(col("dateTaken"), "HH:mm - dd/MM/yyyy"))
    ).withColumn(
        "datePostedFormatted",
        date_format(col("datePosted"), "HH:mm - dd/MM/yyyy")
    )
   
    result_df = formatted_df.select(
        col("photo_url").alias("url"),
        col("owner.username").alias("username"),
        col("tags_array").alias("tags"),
        col("views").alias("views"),
        col("title").alias("title"),
        col("dateTakenFormatted").alias("dateTaken"),
        col("datePostedFormatted").alias("datePosted")
    )

    return result_df

