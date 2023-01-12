from pyspark.sql import functions as F
from pipeline.extract import S3Extract
from pipeline.load import Table, HiveLoad


ext = S3Extract("ods_airports")
df = ext.extract(file_dir="airports").select(
        F.get_json_object(F.col("data"), "$.airport_id").alias("airport_id"),
        F.get_json_object(F.col("data"), "$.name").alias("airport_name"),
        F.get_json_object(F.col("data"), "$.city").alias("airport_city"),
        F.get_json_object(F.col("data"), "$.country").alias("airport_country"),
        F.get_json_object(F.col("data"), "$.iata").alias("airport_iata"),
        F.get_json_object(F.col("data"), "$.icao").alias("airport_icao"),
        F.get_json_object(F.col("data"), "$.latitude").alias("airport_latitude"),
        F.get_json_object(F.col("data"), "$.longitude").alias("airport_longitude"),
        F.get_json_object(F.col("data"), "$.altitude").alias("airport_altitude"),
        F.get_json_object(F.col("data"), "$.timezone").alias("airport_timezone"),
        F.get_json_object(F.col("data"), "$.dst").alias("airport_dst"),
        F.get_json_object(F.col("data"), "$.tz").alias("airport_tz"),
        F.get_json_object(F.col("data"), "$.type").alias("airport_type"),
        F.get_json_object(F.col("data"), "$.source").alias("airport_source"),
        F.get_json_object(F.col("data"), "$.utc_created_dttm").alias("utc_created_dttm"),
        F.get_json_object(F.col("data"), "$.utc_updated_dttm").alias("utc_updated_dttm"),
    )

load_table = Table(schema='default', table_name='airport')

HiveLoad(df, load_table, ext.spark).replace_by_snapshot()
