from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.materialized_view(
    comment = "MV for cancellation analytics - counts, avg distance etc by city, data etc"
)
def gold_cancellation_analysis(): 
    return (
        spark.read.table('silver_ride_events')
        .filter(F.col("is_cancelled") == True)
        .groupBy("event_date", "city", "status", "cancellation_reason")
        .agg(
            F.count("ride_id").alias("cancellation_count"),
            F.round(F.avg("surge_multiplier"), 2).alias("avg_surge_at_cancel"),
            F.round(F.avg("distance_km"), 1).alias("avg_distance_km"),
        )
        .orderBy(F.col("cancellation_count").desc())
    )
