from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import explode, coalesce, lit, col
from pyspark.sql.types import StructType
from pyspark.sql.dataframe import DataFrame
import logging
import argparse

def explode_statistics(df: DataFrame) -> DataFrame:
    """
    Explode the statistics column to separate rows
    Each row will contain a single statistic of a player for a given team
    """
    logging.info("Exploding statistics column")

    return df.withColumn("stat", explode(df["statistics"])).drop("statistics")

def flatten_and_rename(df: DataFrame) -> DataFrame:
    """
    Flatten the DataFrame and rename the columns
    
    Player columns will be renamed to player_<column_name>
    Statistic columns parsed recursively and renamed to previous_name minus the stat. prefix
    Statistic field flattening will be done recursively

    All . in the column names will be replaced with _
    """
    logging.info("Flattening and renaming columns")

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

def set_default_values(df: DataFrame) -> DataFrame:
    """
    Set default values for missing fields
    Must be called after dropping null values
    """
    logging.info("Setting default values for missing fields")

    default_values = {
        "boolean": False,
        "integer": 0,
        "long": 0,
        "float": 0.0,
        "double": 0.0,
        "string": "Unknown"
    }

    defaults = {}
    for field in df.schema.fields:
        field_type = field.dataType.typeName()
        if field_type in default_values:
            defaults[field.name] = default_values[field_type]

    return df.fillna(defaults)

def drop_null_required_columns(df: DataFrame) -> DataFrame:
    """
    Drop rows with null values in required columns
    """
    logging.info("Dropping rows with null values in required columns")

    required_columns = [
        "player_id",
        "player_firstname",
        "player_lastname",
        "player_age",
        "player_nationality",
        "player_weight",
        "player_height",
        "league_id",
        "league_name",
        "league_season",
        "team_id",
        "team_name",
    ]

    return df.dropna(subset=required_columns)

def remove_duplicates(df: DataFrame) -> DataFrame:
    """
    Remove duplicate rows
    Qualifies a row as duplicate if player_id, team_id and league_id are the same
    """
    logging.info("Removing duplicate rows")

    return df.dropDuplicates(["player_id", "team_id", "league_id"])

def main():
    logging.info("Starting Football Players Transformations")
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True, help="Path to the input data")
    parser.add_argument("--output-path", required=True, help="Path to the output data")
    parser.add_argument("--format", required=True, help="Format of the data")
    args = parser.parse_args()

    logging.info(f"Arguments received: {args}")

    spark: SparkSession = SparkSession.builder.appName("Football Players Transformations").getOrCreate()

    df = spark.read.format(args.format).load(args.input_path)

    exploded_df = explode_statistics(df)
    player_data_df = flatten_and_rename(exploded_df)
    player_data_df = drop_null_required_columns(player_data_df)
    player_data_df = set_default_values(player_data_df)
    player_data_df = remove_duplicates(player_data_df)

    player_data_df.write.mode("overwrite").format(args.format).save(args.output_path)

    spark.stop()

if __name__ == '__main__':
    main()