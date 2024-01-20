use polars::prelude::*;
use std::time::Instant;
use threadpool::ThreadPool;


fn filter_df(csv_df: &DataFrame, i: &f64) -> LazyFrame {
    // Filter on multiple columns
    let filtered_df = csv_df.clone().lazy().filter(
        col("a").gt(0.2)
        .and(col("b").lt(0.8))
        .and(col("c").gt(0.5))
        .and(col("d").lt(0.5))
        .and(col("e").neq(*i)) // additional non equality check, value from first row csv
    );
    filtered_df
}

fn calculate_df(filtered_df: LazyFrame, i: &f64) -> DataFrame {
    // Mimic some euclidean distance type calculation
    let calculated_df = filtered_df.select(
        [
            (
                (col("a") / col("a").max()) / (lit(*i) / col("a").max())
            ).pow(2).sqrt()
        ]
    ).collect().unwrap();
    calculated_df
}

fn main() {
    let start_time = Instant::now();

    let csv_file = std::env::var("CSV_FILE").expect("Set env CSV_FILE error");

    let csv_df = CsvReader::from_path(csv_file).unwrap().finish().unwrap();
    let csv_df = Arc::new(csv_df);

    const LOOP_SIZE: i16 = 1000;
    const NUM_THREADS: usize = 8;

    let pool = ThreadPool::new(NUM_THREADS);

    for i in 0..LOOP_SIZE {
        let csv_df = Arc::clone(&csv_df);

        pool.execute(move || {
            let scaled_i = f64::from(i) / f64::from(LOOP_SIZE);
            let filtered_df = filter_df(&csv_df, &scaled_i);
            let _calculated_df = calculate_df(filtered_df, &scaled_i);
        });
    };

    pool.join();

    println!("Finished in {}ms", start_time.elapsed().as_millis());
}