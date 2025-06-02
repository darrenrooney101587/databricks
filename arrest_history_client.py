from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


class ArrestHistoryClient:
    def __init__(self, spark=None):
        self.spark = spark or SparkSession.builder.getOrCreate()

    def get_arrest_history(self):
        return self.spark.read.table("arrest.arrest.arrest_charges_historic_2022_05_13")

    def get_arrest_history_warrant_codes(self):
        return (
            self.get_arrest_history()
            .groupBy(col("arrest_warrant_code"))
            .agg(
                count("arrest_warrant_code").alias("warrant_count")
            )
        )
