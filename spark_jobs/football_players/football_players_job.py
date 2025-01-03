from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import explode, coalesce, lit, col
from pyspark.sql.types import StructType
from pyspark.sql.dataframe import DataFrame
from typing import List
import argparse

def explode_statistics(df: DataFrame) -> DataFrame:
    return df.withColumn("stat", explode(df["statistics"])).drop("statistics")

def flatten_and_rename(df: DataFrame) -> DataFrame:
    renamed_player_columns = [
        col(f"`{field}`").alias(field.replace(".", "_")) for field in df.columns if field != "stat"
    ]

    flatten_stat_columns = []

    def flatten_and_rename_stats_columns(struct: StructType, prefix=''):
        for field in struct.fields:
            if isinstance(field.dataType, StructType):
                flatten_and_rename_stats_columns(field.dataType, prefix + field.name + ".")
            else:
                flatten_stat_columns.append(col("stat." + prefix + field.name).alias((prefix + field.name).replace(".", "_")))

    flatten_and_rename_stats_columns(df.schema["stat"].dataType)

    all_columns = renamed_player_columns + flatten_stat_columns


    return df.select(*all_columns)
    

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True, help="Path to the input data")
    parser.add_argument("--output-path", required=True, help="Path to the output data")
    parser.add_argument("--format", required=True, help="Format of the data")
    args = parser.parse_args()

    spark: SparkSession = SparkSession.builder.appName("Football Players Transformations").getOrCreate()

    # Read the input data
    df = spark.read.format(args.format).load(args.input_path)

    exploded_df = explode_statistics(df)

    player_data_df = flatten_and_rename(exploded_df)


    player_data_df.printSchema()


    spark.stop()

if __name__ == '__main__':
    main()