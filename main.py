import os
import polars as pl
import time


def main():
    start_time = time.perf_counter() * 1000

    csv_file = os.getenv("CSV_FILE")

    csv_df = pl.read_csv(csv_file)
    print(f"Csv read in {time.perf_counter() * 1000 - start_time}ms")

    print(f"Loaded df {csv_df}")
    print(f"Df describe {csv_df.describe()}")

    filtered_df = csv_df.filter(
        pl.col("a").gt(0.2) &
        pl.col("b").lt(0.8) &
        pl.col("c").gt(0.5) &
        pl.col("d").lt(0.5) &
        pl.col("e").ne(0.5182602093634714) # additional non equality check, value from first row csv
    )
    print(f"Filtered df {filtered_df}")

    # Mimic some euclidean distance type calculation
    calculated_df = filtered_df.select(
        (
            (pl.col("a") / pl.col("a").max()) / (0.5 / pl.col("a").max())
        ).pow(2).sqrt()
    )
    print(f"Calculated df {calculated_df}")

    print(f"Finished in {(time.perf_counter() * 1000 - start_time)}ms")


if __name__ == "__main__":
    main()