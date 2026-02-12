use std::fs::File;

use aqua_troll_log_reader::{AquaTrollLogError, AquaTrollLogReader};

// Convert log file to csv format
fn main() -> Result<(), AquaTrollLogError> {
    let mut file = File::open(format!(
        "{}/testing/data/win_situ_record.csv",
        env!["CARGO_MANIFEST_DIR"]
    ))?;

    let reader = AquaTrollLogReader::default();
    let log = reader.read_csv(&mut file)?;

    // Write log_data to csv file
    let log_data_csv_file = File::create("ex_csv_data.csv")?;
    log.log_data.write_csv(log_data_csv_file)?;

    Ok(())
}
