from datetime import datetime,timedelta
from functools import reduce
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import col, window, avg
from pyspark.sql.types import StructType, StructField, StringType, FloatType

from minio import Minio
import json
import io

FOLDER_NAME="halfdaysmean"
BUCKET_NAME="aq54bucket"

config = {
  "minio_endpoint": "mystack-minio:9000",
  "minio_username": "admin",
  "minio_password": "adminpassword",
}

minio_client = Minio(config["minio_endpoint"],
                     secure=False,
                     access_key=config["minio_username"],
                     secret_key=config["minio_password"])

spark = SparkSession \
    .builder \
    .appName("aq54") \
    .enableHiveSupport() \
    .getOrCreate()


# Définir le timestamp actuel et le début de la fenêtre
now_timestamp = datetime.now()
window_start_timestamp = now_timestamp - timedelta(hours=12)


schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("CO", FloatType(), True),
    StructField("T", FloatType(), True),
    StructField("T_int", FloatType(), True),
    StructField("NO2", FloatType(), True),
    StructField("O3", FloatType(), True),
    StructField("PM10", FloatType(), True),
    StructField("PM2.5", FloatType(), True),
    StructField("RH", FloatType(), True)
])

# recuperation des données des 12 dernières heures

objects = minio_client.list_objects('aq54bucket', recursive=True)
objects_in_window = [obj for obj in objects if
                     datetime.strptime(obj.split('_')[1].split('.')[0], '%Y-%m-%d %H:%M:%S')]

data_frames = []

print("etablish connection")

# Parcours des fichiers dans la fenêtre temporelle
for object_name in objects_in_window:
    # Charger le fichier JSON depuis Minio
    response = minio_client.get_object('aq54bucket', object_name)
    response_string = response.read().decode('utf-8')
    json_obj = json.loads(response_string)
    print(json_obj['data'][0])

    # Création d'une liste de dictionnaires à partir des données JSON
    data_list = json_obj['data'][0]

    # Création du DataFrame à partir de la liste de dictionnaires
    data_frame = spark.createDataFrame([tuple(data_list.values())],
                                       schema=schema)

    # Sélection des colonnes spécifiques
    data_frame = data_frame.select(
        col("CO"),
        col("T"),
        col("T_int"),
        col("NO2"),
        col("O3"),
        col("PM10"),
        col("`PM2.5`"),
        col("RH")
    )

    # Ajout du DataFrame à la liste
    data_frames.append(data_frame)

# Combinez tous les DataFrames en un seul
# combined_data = data_frames[0].union(*data_frames[1:])
combined_data = reduce(DataFrame.union, data_frames)

print("combinaison finish")

result = combined_data.agg(
    avg("CO").alias("avg_CO"),
    avg("T").alias("avg_T"),
    avg("T_int").alias("avg_T_int"),
    avg("NO2").alias("avg_NO2"),
    avg("O3").alias("avg_O3"),
    avg("PM10").alias("avg_PM10"),
    avg("`PM2.5`").alias("avg_PM2.5"),
    avg("RH").alias("avg_RH")
)

json_content = result.toJSON().collect()[0]

object_name = f"{FOLDER_NAME}/poluant_{datetime.now()}.json"
print("about to save")
minio_client.put_object(BUCKET_NAME, object_name, io.BytesIO(json_content), len(json_content), content_type="application/json")
print("save finish")