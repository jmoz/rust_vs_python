use polars::prelude::*;
use std::time::Instant;


fn main() {
    let start_time = Instant::now();

    let csv_file = std::env::var("CSV_FILE").expect("Set env CSV_FILE error");

    let csv_df = CsvReader::from_path(csv_file).expect("Could not load csv error").has_header(true).finish().expect("Finish error");
    println!("Csv read in {}ms", start_time.elapsed().as_millis());

    // cargo add polars -F fmt,describe
    println!("Loaded df {}", csv_df);
    println!("Df describe {}", csv_df.describe(None).expect("Describe error"));

    // Filter on multiple columns
    let filtered_df = csv_df.clone().lazy().filter(
        col("a").gt(0.2)
        .and(col("b").lt(0.8))
        .and(col("c").gt(0.5))
        .and(col("d").lt(0.5))
        .and(col("e").neq(0.5182602093634714)) // additional non equality check, value from first row csv
    ).collect().expect("Error filtering");
    println!("Filtered df {}", filtered_df);

    // Mimic some euclidean distance type calculation
    let calculated_df = filtered_df.clone().lazy().select(
        [
            (
                (col("a") / col("a").max()) / (lit(0.5) / col("a").max())
            ).pow(2).sqrt()
        ]
    ).collect().expect("Error select");
    println!("Calculated df {}", calculated_df);

    println!("Finished in {}ms", start_time.elapsed().as_millis());
}