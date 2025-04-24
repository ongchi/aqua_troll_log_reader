use std::fs::File;

use arrow::json::ArrayWriter;

use insitu_log_reader::{InSituLogError, InSituLogReader};

// Convert log file to json and csv format
fn main() -> Result<(), InSituLogError> {
    let mut file = File::open(format!(
        "{}/testing/data/win_situ_record.csv",
        env!["CARGO_MANIFEST_DIR"]
    ))?;

    let log = InSituLogReader::from_csv(&mut file)?;

    // Write log_data to json file
    let log_data_json_file = File::create("win_situ_record.json")?;
    let mut csv_writer = ArrayWriter::new(log_data_json_file);
    csv_writer.write(&log.log_data)?;
    csv_writer.finish()?;

    Ok(())
}
