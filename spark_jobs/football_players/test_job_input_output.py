import pyarrow.parquet as pq
import pandas as pd
import glob

def input_test():
    directory = glob.glob("/Users/xelathan/football_players_etl_pipeline/spark_jobs/football_players/input/*.parquet")
    if directory:
        table = pq.read_table(directory[0])
        data : pd.DataFrame = table.to_pandas()

        print(data.columns)

def main():
    directory = glob.glob("/Users/xelathan/football_players_etl_pipeline/spark_jobs/football_players/output/football_data.parquet/part-*.snappy.parquet")

    if directory:
        table = pq.read_table(directory[0])
        data : pd.DataFrame = table.to_pandas()

        print(data.shape[0])

if __name__ == "__main__":
    input_test()