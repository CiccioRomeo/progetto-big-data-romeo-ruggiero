from flask import Flask, request, jsonify
from flask_cors import CORS
import Queries.queries as queries
import Queries.mlqueries as mlqueries
from flask_compress import Compress
from pyspark.sql import DataFrame


mappa_mesi = {
        1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile",
        5: "Maggio", 6: "Giugno", 7: "Luglio", 8: "Agosto",
        9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre"
    }


def create_app(df : DataFrame, df_avatars : DataFrame ):

    app = Flask(__name__)
    CORS(app)
    Compress(app)


    @app.route("/getFirstRows", methods=["GET"])
    def get_first_rows():
        limit = int(request.args.get("limit", 10))
        result_df = queries.get_first_n_rows(df, limit)
        return jsonify([row.asDict() for row in result_df.collect()])


    @app.route("/photosByCoordinates", methods=["GET"])
    def photos_by_coordinates():
        try:
            result_df = queries.count_photos_by_coordinates(df)
            # Utilizzo delle tuple per risparmiare spazio
            return jsonify([[row["latitude"], row["longitude"], row["photoCount"]] for row in result_df.collect()])
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        
    @app.route("/countRows", methods=["GET"])
    def get_count_rows():
        try:
            result= queries.count_rows(df)
            return jsonify(result)
        except Exception as e:
            return jsonify({"error": str(e)}), 500


    @app.route("/photoCountPosted", methods=["GET"])
    def photo_count_posted():
        try:
            result_month = queries.photo_count_by_month_posted(df).collect()
            result_year  = queries.photo_count_by_year_posted(df).collect()

            result = {
                "month_data" : [[mappa_mesi.get(row["month"]), row["count"]] for row in result_month],
                "year_data" : result_year
            }

            return jsonify(result)
        except Exception as e:
            return jsonify({"error": str(e)}), 500


    @app.route("/photoCountTaken", methods=["GET"])
    def photo_count_taken():
        try:
            result_month = queries.photo_count_by_month_taken(df).collect()
            result_year  = queries.photo_count_by_year_taken(df).collect()

            result = {
                "month_data" : [[mappa_mesi.get(row["month"]), row["count"]] for row in result_month],
                "year_data" : result_year
            }
            return jsonify(result)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        

    @app.route("/photoCountHour", methods=["GET"])    
    def photo_count_posted_hour():
        try:
            result_posted = queries.count_photos_posted_per_hour(df).collect()
            result_taken = queries.count_photos_taken_per_hour(df).collect()

            result = {
                "posted" : result_posted,
                "taken" : result_taken
            }

            return jsonify(result)
        
        except Exception as e:
            return jsonify({"error": str(e)}), 500


    @app.route("/photoPostedPerMonthByYear", methods=["GET"])
    def photo_posted_per_month_by_year():
        try:
            # Validate input parameters
            year_param = request.args.get("year")
            type_param = request.args.get("type")

            if not year_param:
                return jsonify("Inserire anno: es. 2007"), 400
            
            try:
                input_year = int(year_param)
            except ValueError:
                return jsonify("L'anno deve essere un numero valido"), 400

            if not type_param:
                return jsonify("Inserire parametro type: posted o taken"), 400

            if type_param not in ["posted", "taken"]:
                return jsonify("Il parametro type deve essere 'posted' o 'taken'"), 400

            if type_param == "posted":
                result_df = queries.photo_posted_per_month_by_year_posted(df, input_year)
            elif type_param == "taken":
                result_df = queries.photo_posted_per_month_by_year_taken(df, input_year)

            query_result = {row["month"]: row["count"] for row in result_df.collect()}

            complete_result = [
                {"month": mappa_mesi.get(month, str(month)), "count": query_result.get(month, 0)}
                for month in range(1, 13)
            ]

            return jsonify(complete_result)
        except Exception as e:
            return jsonify({"error": str(e)}), 500


    @app.route("/averageTimeToPost", methods=["GET"])
    def average_time_to_post():
        try:
            result_df = queries.calculate_average_time_to_post(df)

            return jsonify([row.asDict() for row in result_df.collect()])
        
        except Exception as e:
            return jsonify({"error": str(e)}), 500


    @app.route("/countUsers", methods=["GET"])
    def get_users_count():
        try:
            result =  queries.count_user(df)

            return jsonify(result)
        
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        

    @app.route("/viewStats", methods=["GET"])
    def get_views_stats():
        try:
            result_df = queries.calculate_views_stats(df)

            return jsonify([row.asDict() for row in result_df.collect()])
        
        except Exception as e:
            return jsonify({"error": str(e)}), 500


    @app.route("/commentStats", methods=["GET"])
    def get_comments_stats():
        try:
            result_df = queries.calculate_comments_stats(df)
            
            return jsonify([row.asDict() for row in result_df.collect()])
        
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        

    @app.route("/avgViewsPerYear", methods=["GET"])
    def get_average_views_per_year():
        try:
            result_df = queries.calculate_views_by_year(df)

            return jsonify([row.asDict() for row in result_df.collect()])
        
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        

    @app.route("/avgCommentsPerYear", methods=["GET"])
    def get_average_comments_per_year():
        try:
            result_df = queries.calculate_comments_by_year(df)

            return jsonify([row.asDict() for row in result_df.collect()])
        
        except Exception as e:
            return jsonify({"error": str(e)}), 500


    @app.route("/firstPostPerYear", methods=["GET"])
    def get_first_post_count_per_year():
        try:
            result_df = queries.first_post_per_year_month(df)

            # Raggruppare i risultati per anno
            result_by_year = {}
            for row in result_df.collect():
                year = row["year"]
                month_number = row["month"]
                count = row["count"]

                if year not in result_by_year:
                    result_by_year[year] = []

                result_by_year[year].append({"month": month_number, "count": count})

        
            result = [
                {"year": year, "months": months}
                for year, months in result_by_year.items()
            ]

            return jsonify(result)
        except Exception as e:
            return jsonify({"error": str(e)}), 500



    @app.route("/topTags", methods=["GET"])
    def top_tags():
        try:
            limit = int(request.args.get("limit", 50))
            result_df = queries.get_top_tags(df).limit(limit)

            return jsonify([row.asDict() for row in result_df.collect()])
        
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        

    @app.route("/proUsersDistribution", methods=["GET"])
    def get_pro_users_distribution():
        try:
            result_df = queries.calculate_pro_user_distribution(df)
            return jsonify([row.asDict() for row in result_df.collect()])
        except Exception as e:

            return jsonify({"error": str(e)}), 500


    @app.route("/accuracyDistribution", methods=["GET"])
    def get_accuracy_distribution():
        try:
            result_df = queries.calculate_accuracy_distribution(df)

            return jsonify([row.asDict() for row in result_df.collect()])
        
        except Exception as e:
            return jsonify({"error": str(e)}), 500


    # @app.route("/searchOwner", methods=["GET"])
    # def search_owner_rank():
    #     try:
    #         username = request.args.get("username")

    #         if not username:
    #             return jsonify("error: parametro username mancante"), 400
    #         result_df = queries.search_owner(df,username)
    #         return jsonify([row.asDict() for row in result_df.collect()])
    #     except Exception as e:
    #         return jsonify({"error": str(e)}), 500
        

    @app.route("/searchOwnerCam", methods=["GET"])
    def search_owner_rank_C():
        try:
            username = request.args.get("username")

            if not username:
                return jsonify("error: parametro username mancante"), 400
            
            result_df = queries.search_owner_with_cameras(df, df_avatars, username)

            return jsonify([row.asDict() for row in result_df.collect()])
        
        except Exception as e:
            return jsonify({"error": str(e)}), 500



    @app.route("/top50Owners", methods=["GET"])
    def get_top_50_owners():
        try:
            result_df = queries.top_50_owners(df)

            return jsonify([row.asDict() for row in result_df.collect()])
        
        except Exception as e:
            return jsonify({"error": str(e)}), 500


    @app.route('/runKMeans', methods=['GET'])
    def run_kmeans():
        try:
         
            k = int(request.args.get("k", 5))

            result= mlqueries.run_kmeans_clustering(df, k)

            return jsonify(result)
        
        except Exception as e:
            return jsonify({"error": str(e)}), 500


    @app.route("/tagAssociationRules", methods=["POST"])
    def calculate_rules():
        try:

            data = request.get_json()
            min_support = data.get('min_support', 0.2)
            min_confidence = data.get('min_confidence', 0.6)
            target_tags = data.get('target_tags', None)

            result_df =mlqueries.calculate_and_filter_association_rules2(
                df, 
                min_support=min_support, 
                min_confidence=min_confidence, 
                target_tags=target_tags
            )

            return jsonify([row.asDict() for row in result_df.collect()])

        except Exception as e:
            return jsonify({"error": str(e)}), 500
        

    @app.route('/searchPhotos', methods=['POST'])
    def search_photos_endpoint():
        try:
            
            data = request.get_json()  
            if data is None:
                return jsonify({"error": "No data provided"}), 400

            keyword = data.get('keyword')
            data_inizio = data.get('dataInizio')
            data_fine = data.get('dataFine')

            if data_fine is not None and data_inizio is not  None:
                if data_fine < data_inizio:
                    return jsonify("error: data fine non puo essere precedente di data inizio"), 400
            
            tag_list = data.get('tag_list', [])  
            limit = int(request.args.get("limit", 100))

            result_df = queries.search_photos(df, 
                                              keyword, 
                                              data_inizio,
                                              data_fine,  
                                              tag_list).limit(limit)

            return jsonify([row.asDict() for row in result_df.collect()])
        
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        

    @app.route('/years', methods=['GET'])
    def get_years_list():
        try:
            result_df = queries.get_years(df)
            return jsonify([row.asDict() for row in result_df.collect()])
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        

    @app.route("/getTopBrandAndCameras", methods=["GET"])
    def get_top_barnd_cameras():
        try:
            result_df = queries.top_brands_with_models(df)
            return jsonify([row.asDict() for row in result_df.collect()])
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        

    @app.route("/topCamerasPerYear", methods=["GET"])
    def get_top_barnd_cameras_per_year():
        try:
            result_df = queries.top_models_per_year(df)
            return jsonify([row.asDict() for row in result_df.collect()])
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        

    @app.route("/brandCount", methods=["GET"])
    def get_brand_count():
        try:
            result = queries.count_brand(df)
            return jsonify(result)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        

    @app.route("/searchBrands", methods=["GET"])
    def search_brands_by_name():
        try:
            brand = request.args.get("brand")
            
            if not brand:
                return jsonify("error: parametro brand mancante"), 400
            
            result_df = queries.search_brands(df,brand)
            return jsonify(result_df.collect())
        except Exception as e:
            return jsonify({"error": str(e)}), 500


    @app.route("/searchBrandsInfo", methods=["GET"])
    def search_brands_info_by_name():
        try:
            brand = request.args.get("brand")
            
            if not brand:
                return jsonify("error: parametro brand mancante"), 400
            
            result_df = queries.get_cameras_photo_count_by_brand(df,brand)
            return jsonify(result_df.collect())
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        

    return app




