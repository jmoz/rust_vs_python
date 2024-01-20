import os
import polars as pl
import time


def filter_df(csv_df, i):
    filtered_df = csv_df.filter(
        pl.col("a").gt(0.2)
        & pl.col("b").lt(0.8)
        & pl.col("c").gt(0.5)
        & pl.col("d").lt(0.5)
        & pl.col("e").ne(i)  # additional non equality check, value from first row csv
    )
    return filtered_df


def calculate_df(filtered_df, i):
    # Mimic some euclidean distance type calculation
    calculated_df = filtered_df.select(
        ((pl.col("a") / pl.col("a").max()) / (i / pl.col("a").max())).pow(2).sqrt()
    )
    return calculate_df


def main():
    start_time = time.perf_counter() * 1000

    csv_df = pl.read_csv(os.getenv("CSV_FILE"))

    # some standard looping to mimic running dataframe calcs on multiple items
    LOOP_SIZE = 1000
    for i in range(0, LOOP_SIZE):
        scaled_i = i / LOOP_SIZE
        filtered_df = filter_df(csv_df, scaled_i)
        calculated_df = calculate_df(filtered_df, scaled_i)

    print(f"Finished in {(time.perf_counter() * 1000 - start_time)}ms")


if __name__ == "__main__":
    main()
