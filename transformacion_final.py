#!/usr/bin/env python3

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

from pyspark.sql import functions as F, types as T

##leo el archivo csv ingestado  desde HDFS y lo cargo en un dataframe.
car_rental = spark.read.options(header="true").csv("hdfs://172.17.0.2:9000/ingest/CarRentalData.csv")
geo_ref = spark.read.options(header="true", sep=";").csv("hdfs://172.17.0.2:9000/ingest/geo_ref.csv")



# Modifico los nombres de las columnas
for col_name in car_rental.columns:
    car_rental = car_rental.withColumnRenamed(col_name, col_name.replace(".", "_").replace(" ", "_"))


car_rental = (
    car_rental

    # Redondea los float de ‘rating’ y castear a int.
    .withColumn("rating", F.round(F.col("rating").cast(T.FloatType())).cast(T.IntegerType()))

    # Elimina los registros con rating nulo.
    .filter(F.col("rating").isNotNull())

    # Cambio mayúsculas por minúsculas en ‘fuelType’
    .withColumn("fuelType", F.lower(F.col("fuelType")))
    )

# Seleccionar las columnas requeridas de geo_ref antes del join
geo_ref_selected = geo_ref.select(F.col("ste_stusps_code").alias("location_state"), F.col("ste_name").alias("state_name"))

# Join con el geo_ref seleccionado usando el código del estado
car_rental_joined = car_rental.join(geo_ref_selected, on="location_state", how="inner")


# Excluir registros de Texas
car_rental_filtered = car_rental_joined.filter(F.col("state_name") != "Texas")

# Seleccionar las columnas requeridas y renombrar algunas de ellas

final_df = car_rental_filtered.select(
    "fuelType",
    "rating",
    F.col("renterTripsTaken").cast("int"),
    F.col("reviewCount").cast("int"),
    F.col("location_city").alias("city"),
    "state_name",
    F.col("owner_id").cast("int"),
    F.col("rate_daily").cast("int"),
    F.col("vehicle_make").alias("make"),
    F.col("vehicle_model").alias("model"),
    F.col("vehicle_year").cast("int")
)

#df final para insertar en Hive.
final_df.write.mode("overwrite").saveAsTable("car_rental_db.car_rental_analytics")