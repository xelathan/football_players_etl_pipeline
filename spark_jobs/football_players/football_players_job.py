from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, coalesce, lit, col
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True, help="Path to the input data")
    parser.add_argument("--output-path", required=True, help="Path to the output data")
    parser.add_argument("--format", required=True, help="Format of the data")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Football Players Transformations").getOrCreate()

    # Define player fields and stats with their default values
    players_fields = {
        "player.id": (None, "int"),
        "player.firstname": (None, "string"),
        "player.lastname": (None, "string"),
        "player.age": (None, "int"),
        "player.nationality": (None, "string"),
        "player.height": (None, "int"),
        "player.weight": (None, "int"),
        "player.injured": (False, "boolean"),
    }

    stat_fields = {
        # Team fields
        "team.id": (None, "int"),
        "team.name": (None, "string"),
        # League fields
        "league.id": (None, "int"),
        "league.name": (None, "string"),
        "league.country": (None, "string"),
        "league.season": (None, "int"),
        # Games fields
        "games.appearences": (0, "int"),
        "games.lineups": (0, "int"),
        "games.minutes": (0, "int"),
        "games.number": (0, "int"),
        "games.position": (None, "string"),
        "games.rating": (0, "float"),
        "games.captain": (False, "boolean"),
        # Substitutes fields
        "substitutes.in": (0, "int"),
        "substitutes.out": (0, "int"),
        "substitutes.bench": (0, "int"),
        # Shots fields
        "shots.total": (0, "int"),
        "shots.on": (0, "int"),
        # Goals fields
        "goals.total": (0, "int"),
        "goals.assists": (0, "int"),
        # Passes fields
        "passes.total": (0, "int"),
        "passes.key": (0, "int"),
        "passes.accuracy": (None, "string"),
        # Tackles fields
        "tackles.total": (0, "int"),
        "tackles.blocks": (0, "int"),
        "tackles.interceptions": (0, "int"),
        # Duels fields
        "duels.total": (0, "int"),
        "duels.won": (0, "int"),
        # Dribbles fields
        "dribbles.attempts": (0, "int"),
        "dribbles.success": (0, "int"),
        # Fouls fields
        "fouls.drawn": (0, "int"),
        "fouls.committed": (0, "int"),
        # Cards fields
        "cards.yellow": (0, "int"),
        "cards.yellowred": (0, "int"),
        "cards.red": (0, "int"),
        # Penalty fields
        "penalty.won": (0, "int"),
        "penalty.scored": (0, "int")
    }

    # Creating the stat columns with coalesce to apply default values
    stat_columns = [
        coalesce(f"stat.{field}", lit(default_value)).alias(field)
        for field, (default_value, _) in stat_fields.items() if default_value is not None
    ]

    # Read the input data
    df = spark.read.format(args.format).load(args.input_path)
    df.printSchema()

    # exploded_df = df.withColumn("stat", explode(df["statistics"]))

    # # Apply coalesce for the player fields that need defaults and are not None
    # player_columns = [
    #     coalesce(field, lit(default_value))
    #     for field, (default_value, _) in players_fields.items() if default_value is not None
    # ]

    # print(df.columns)

    # # Create the player DataFrame selecting only the relevant fields
    # player_df = exploded_df.select(*player_columns, *stat_columns)

    # # Filter out rows where any field is None
    # for field in players_fields.keys():
    #     player_df = player_df.filter(col(field).isNotNull())

    # for field in stat_fields.keys():
    #     player_df = player_df.filter(col(field).isNotNull())

    # # Write the final output
    # player_df.write.format(args.format).mode("overwrite").save(args.output_path)

    spark.stop()

if __name__ == '__main__':
    main()