from abc import ABC, abstractmethod
from datetime import datetime

# import clickhouse_connect
from pyspark.sql import DataFrame, SparkSession, functions as F


class Table:
    def __init__(self, schema: str, table_name: str):
        self.schema = schema
        self.table_name = table_name


# TODO: add partitioned tables handling
class BaseLoad(ABC):
    def __init__(self,
                 df: DataFrame,
                 table: Table,
                 spark: SparkSession
                 ) -> None:
        self.df = df.withColumn('utc_upload_dttm', F.lit(str(datetime.now())))
        self.table = table
        self.full_table_name = self.table.schema + '.' + self.table.table_name
        self.spark = spark

    def table_exists_assurance(self):
        schema = ', '.join(' '.join(x) for x in self.df.dtypes)
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.full_table_name} ({schema})")

    @abstractmethod
    def replace_by_snapshot(self, *args, **kwargs):
        self.table_exists_assurance()

    @abstractmethod
    def replace_by_period(self, *args, **kwargs):
        self.table_exists_assurance()


class HiveLoad(BaseLoad):
    def __init__(self, df: DataFrame, table: Table, spark: SparkSession) -> None:
        super().__init__(df, table, spark)

    def replace_by_snapshot(self, *args, **kwargs):
        super().replace_by_snapshot()

        self.spark.sql(f"TRUNCATE TABLE {self.full_table_name}")

        self.df.createOrReplaceTempView(self.table.table_name)
        self.spark.sql(f"INSERT INTO TABLE {self.full_table_name} SELECT * FROM {self.table.table_name}")

    def replace_by_period(self, *args, **kwargs):
        pass


class ClickhouseLoad(BaseLoad):
    def __init__(self, df: DataFrame, table: Table, spark: SparkSession) -> None:
        super().__init__(df, table, spark)

    def replace_by_snapshot(self, *args, **kwargs):
        super().replace_by_snapshot()

    def replace_by_period(self, *args, **kwargs):
        pass
