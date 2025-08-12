mod cli;
mod models;
mod processor;

use cli::Args;
use processor::EtlProcessor;
use std::error::Error;
use std::time::Instant;
use clap::Parser;

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    println!("Starting ETL process...");
    println!("Input: {}", args.input);
    println!("Output: {}", args.output);
    println!("Min range: {}", args.min_range);
    println!("Mode: {}", args.mode);

    let start_time = Instant::now();

    let result = match args.mode.as_str() {
        "sequential" => {
            EtlProcessor::process_sequential(&args.input, &args.output, args.min_range)
        }
        "parallel" => {
            EtlProcessor::process_parallel(&args.input, &args.output, args.min_range)
        }
        "batched" => {
            EtlProcessor::process_batched(&args.input, &args.output, args.min_range, args.batch_size)
        }
        _ => {
            eprintln!("Unknown mode: {}. Use: sequential, parallel, batched", args.mode);
            std::process::exit(1);
        }
    };

    let duration = start_time.elapsed();
    println!("ETL completed in {:?}", duration);

    result
}