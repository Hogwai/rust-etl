use crate::models::VehicleRecord;
use csv::{Reader, Writer};
use rayon::prelude::*;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, BufWriter};

pub struct EtlProcessor;

impl EtlProcessor {
    pub fn process_sequential(
        input_path: &str,
        output_path: &str,
        min_range: u16,
    ) -> Result<(), Box<dyn Error>> {
        let reader_file = File::open(input_path)?;
        let buf_reader = BufReader::new(reader_file);
        let mut reader = Reader::from_reader(buf_reader);

        let writer_file = File::create(output_path)?;
        let buf_writer = BufWriter::new(writer_file);
        let mut writer = Writer::from_writer(buf_writer);

        let mut processed_count = 0;
        let mut eligible_count = 0;
        let mut invalid_count = 0;

        for result in reader.deserialize() {
            match result {
                Ok(record) => {
                    let record: VehicleRecord = record;
                    processed_count += 1;

                    if !record.has_valid_data() {
                        invalid_count += 1;
                        continue;
                    }

                    if record.is_eligible(min_range) {
                        writer.serialize(record)?;
                        eligible_count += 1;
                    }
                }
                Err(e) => {
                    eprintln!("Warning: Skipping malformed record: {}", e);
                    invalid_count += 1;
                }
            }
        }

        writer.flush()?;
        println!("Processed: {} records", processed_count);
        println!("Eligible: {} records", eligible_count);
        println!("Invalid/Skipped: {} records", invalid_count);
        Ok(())
    }

    pub fn process_parallel(
        input_path: &str,
        output_path: &str,
        min_range: u16,
    ) -> Result<(), Box<dyn Error>> {
        let reader_file = File::open(input_path)?;
        let mut reader = Reader::from_reader(reader_file);

        // Collect all records with error handling
        let mut valid_records = Vec::new();
        let mut invalid_count = 0;

        for result in reader.deserialize() {
            match result {
                Ok(record) => {
                    let record: VehicleRecord = record;
                    if record.has_valid_data() {
                        valid_records.push(record);
                    } else {
                        invalid_count += 1;
                    }
                }
                Err(e) => {
                    eprintln!("Warning: Skipping malformed record: {}", e);
                    invalid_count += 1;
                }
            }
        }

        println!("Valid records for processing: {}", valid_records.len());
        println!("Invalid/Skipped: {} records", invalid_count);

        // Filter in parallel
        let filtered: Vec<VehicleRecord> = valid_records
            .into_par_iter()
            .filter(|record| record.is_eligible(min_range))
            .collect();

        let eligible_count = filtered.len();

        // Write results
        let writer_file = File::create(output_path)?;
        let mut writer = Writer::from_writer(writer_file);

        for record in filtered {
            writer.serialize(record)?;
        }

        writer.flush()?;
        println!("Eligible records written: {}", eligible_count);
        Ok(())
    }

    pub fn process_batched(
        input_path: &str,
        output_path: &str,
        min_range: u16,
        batch_size: usize,
    ) -> Result<(), Box<dyn Error>> {
        let reader_file = File::open(input_path)?;
        let mut reader = Reader::from_reader(reader_file);

        let writer_file = File::create(output_path)?;
        let mut writer = Writer::from_writer(writer_file);

        let mut batch = Vec::new();
        let mut total_processed = 0;
        let mut total_eligible = 0;

        for result in reader.deserialize() {
            match result {
                Ok(record) => {
                    let record: VehicleRecord = record;
                    if record.has_valid_data() {
                        batch.push(record);
                    }

                    // Process batch when full
                    if batch.len() >= batch_size {
                        let eligible: Vec<VehicleRecord> = batch
                            .par_iter()
                            .filter(|record| record.is_eligible(min_range))
                            .cloned()
                            .collect();
                        let mut eligible_batch = 0;

                        for record in eligible {
                            writer.serialize(record)?;
                            eligible_batch += 1;
                        }
                        total_eligible += eligible_batch;
                        total_processed += batch.len();
                        batch.clear();

                        // Progress indicator
                        if total_processed % 100_000 == 0 {
                            println!("Processed: {} records", total_processed);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Warning: Skipping malformed record: {}", e);
                }
            }
        }

        // Process remaining batch
        if !batch.is_empty() {
            let eligible: Vec<VehicleRecord> = batch
                .par_iter()
                .filter(|record| record.is_eligible(min_range))
                .cloned()
                .collect();
            let mut eligible_batch = 0;

            for record in eligible {
                writer.serialize(record)?;
                eligible_batch += 1;
            }
            total_eligible += eligible_batch;
            total_processed += batch.len();
        }

        writer.flush()?;
        println!("Total processed: {} records", total_processed);
        println!("Total eligible: {} records", total_eligible);
        Ok(())
    }
}