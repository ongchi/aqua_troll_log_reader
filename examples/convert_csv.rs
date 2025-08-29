use std::fs::File;

use arrow::csv::Writer as CsvWriter;

use aqua_troll_log_reader::{AquaTrollLogError, AquaTrollLogReader};

// Convert log file to json and csv format
fn main() -> Result<(), AquaTrollLogError> {
    let mut file = File::open(format!(
        "{}/testing/data/win_situ_record.csv",
        env!["CARGO_MANIFEST_DIR"]
    ))?;

    let log = AquaTrollLogReader::from_csv(&mut file)?;

    // Write log_data to json file
    let log_data_json_file = File::create("ex_csv_data.csv")?;
    CsvWriter::new(log_data_json_file).write(&log.log_data)?;

    Ok(())
}
