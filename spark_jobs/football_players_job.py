from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("FootballPlayers").getOrCreate()

if __name__ == '__main__':
    main()