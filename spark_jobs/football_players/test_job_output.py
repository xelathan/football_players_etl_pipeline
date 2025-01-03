import pyarrow.parquet as pq
import pandas as pd
import glob

def main():
    directory = glob.glob("/Users/xelathan/football_players_etl_pipeline/spark_jobs/football_players/output/football_data.parquet/part-*.snappy.parquet")

    if directory:
        table = pq.read_table(directory[0])
        data : pd.DataFrame = table.to_pandas()

        print(data.head())

if __name__ == "__main__":
    main()