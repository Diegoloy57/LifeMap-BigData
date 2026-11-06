
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    split,
    explode,
    trim,
    count,
    sum as spark_sum,
    when,
    round as spark_round,
    lit,
)


# ============================================================
# LIFEMAP - PROCESAMIENTO SPARK
# Estadísticas descriptivas de negocios Yelp
# ============================================================

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://mongo:27017"
)

DATABASE_NAME = "lifemap"
SOURCE_COLLECTION = "businesses"
RESULT_COLLECTION = "closure_stats"

SELECTED_CITY = os.getenv(
    "LIFEMAP_CITY",
    "Philadelphia"
)


# ============================================================
# 1. CREAR SESIÓN SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("LifeMapClosureAnalysis")
    .config(
        "spark.mongodb.read.connection.uri",
        MONGO_URI
    )
    .config(
        "spark.mongodb.write.connection.uri",
        MONGO_URI
    )
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 55)
print("       LIFEMAP - ANALISIS CON APACHE SPARK")
print("=" * 55)


try:

    # ========================================================
    # 2. LEER LOS NEGOCIOS DESDE MONGODB
    # ========================================================

    print("\n1. Leyendo negocios desde MongoDB...")

    df = (
        spark.read
        .format("mongodb")
        .option("database", DATABASE_NAME)
        .option("collection", SOURCE_COLLECTION)
        .load()
    )

    print(f"Negocios totales: {df.count():,}")

    # ========================================================
    # 3. FILTRAR FILADELFIA
    # ========================================================

    print(f"\n2. Filtrando ciudad: {SELECTED_CITY}")

    city_df = (
        df
        .filter(col("city") == SELECTED_CITY)
        .filter(col("business_id").isNotNull())
        .filter(col("is_open").isin(0, 1))
        .select(
            "business_id",
            "name",
            "city",
            "categories",
            "is_open",
            "stars",
            "review_count"
        )
        .dropDuplicates(["business_id"])
        .cache()
    )

    total_city = city_df.count()

    print(f"Negocios válidos en {SELECTED_CITY}: {total_city:,}")

    if total_city == 0:
        raise ValueError(
            "No hay negocios válidos para la ciudad seleccionada."
        )

    # ========================================================
    # 4. ESTADO GENERAL DE LOS NEGOCIOS
    # ========================================================

    print("\n3. Calculando negocios abiertos y cerrados...")

    city_summary = (
        city_df
        .agg(
            count("*").alias("total_businesses"),
            spark_sum(
                when(col("is_open") == 0, 1).otherwise(0)
            ).alias("closed_businesses"),
            spark_sum(
                when(col("is_open") == 1, 1).otherwise(0)
            ).alias("open_businesses")
        )
        .withColumn(
            "closed_percentage",
            spark_round(
                col("closed_businesses")
                / col("total_businesses") * 100,
                2
            )
        )
    )

    print("\nResumen general:")
    city_summary.show(truncate=False)

    # ========================================================
    # 5. PREPARAR CATEGORÍAS
    # ========================================================

    print("\n4. Separando categorías de Yelp...")

    categories_df = (
        city_df
        .filter(col("categories").isNotNull())
        .withColumn(
            "category",
            explode(split(col("categories"), ","))
        )
        .withColumn(
            "category",
            trim(col("category"))
        )
        .filter(col("category") != "")
        .select(
            "business_id",
            "category",
            "is_open"
        )
        .dropDuplicates(["business_id", "category"])
        .cache()
    )

    # ========================================================
    # 6. ESTADÍSTICAS POR CATEGORÍA CON DATAFRAME
    # ========================================================

    print("\n5. Calculando cierres por categoría...")

    category_stats = (
        categories_df
        .groupBy("category")
        .agg(
            count("*").alias("total_businesses"),
            spark_sum(
                when(col("is_open") == 0, 1).otherwise(0)
            ).alias("closed_businesses"),
            spark_sum(
                when(col("is_open") == 1, 1).otherwise(0)
            ).alias("open_businesses")
        )
        .withColumn(
            "closed_percentage",
            spark_round(
                col("closed_businesses")
                / col("total_businesses") * 100,
                2
            )
        )
    )

    category_stats.orderBy(
        col("total_businesses").desc()
    ).show(15, truncate=False)

    # ========================================================
    # 7. MAPREDUCE CON RDD
    # ========================================================

    print("\n6. Validando resultados con MapReduce...")

    rdd_stats = (
        categories_df
        .select("category", "is_open")
        .rdd
        .map(
            lambda row: (
                row["category"],
                (
                    1,
                    1 if row["is_open"] == 0 else 0,
                    1 if row["is_open"] == 1 else 0
                )
            )
        )
        .reduceByKey(
            lambda a, b: (
                a[0] + b[0],
                a[1] + b[1],
                a[2] + b[2]
            ),
            numPartitions=4
        )
        .collectAsMap()
    )

    dataframe_stats = {
        row["category"]: (
            row["total_businesses"],
            row["closed_businesses"],
            row["open_businesses"]
        )
        for row in category_stats.collect()
    }

    if dict(rdd_stats) != dataframe_stats:
        raise ValueError(
            "Los resultados de MapReduce y DataFrame no coinciden."
        )

    print("VALIDACIÓN EXITOSA:")
    print("MapReduce y DataFrame producen los mismos resultados.")

    # ========================================================
    # 8. PREPARAR RESULTADOS PARA MONGODB
    # ========================================================

    print("\n7. Preparando resultados...")

    output_df = (
        category_stats
        .withColumn(
            "city",
            lit(SELECTED_CITY)
        )
        .withColumn(
            "metric_type",
            lit("closure_proportion")
        )
        .withColumn(
            "business_years",
            lit(None).cast("double")
        )
        .withColumn(
            "closure_rate_per_100_business_years",
            lit(None).cast("double")
        )
        .withColumn(
            "calculation_status",
            lit("partial_without_review_dates")
        )
        .select(
            "city",
            "category",
            "total_businesses",
            "open_businesses",
            "closed_businesses",
            "closed_percentage",
            "metric_type",
            "business_years",
            "closure_rate_per_100_business_years",
            "calculation_status"
        )
    )

    # ========================================================
    # 9. GUARDAR EN MONGODB
    # ========================================================

    print(
        f"\n8. Guardando en "
        f"{DATABASE_NAME}.{RESULT_COLLECTION}..."
    )

    (
        output_df
        .write
        .format("mongodb")
        .mode("overwrite")
        .option("database", DATABASE_NAME)
        .option("collection", RESULT_COLLECTION)
        .save()
    )

    print("Resultados guardados correctamente.")

    # ========================================================
    # 10. RESUMEN FINAL
    # ========================================================

    total_categories = output_df.count()

    print("\n" + "=" * 55)
    print("              RESULTADO FINAL")
    print("=" * 55)

    print(f"Ciudad: {SELECTED_CITY}")
    print(f"Negocios analizados: {total_city:,}")
    print(f"Categorías analizadas: {total_categories:,}")
    print(f"Colección: {DATABASE_NAME}.{RESULT_COLLECTION}")

    print("Validación MapReduce: OK")

    print(
        "IMPORTANTE: Los resultados corresponden a "
        "proporciones de cierre, no a tasas por negocio-años."
    )

    print("=" * 55)

finally:

    spark.stop()
