use std::{
    fs::File,
    io::{BufReader, Write},
};

use arrow::csv::Writer as CsvWriter;

use insitu_log_reader::{InSituLogError, InSituLogReader};

// convert zipped html log file to json and csv files
fn main() -> Result<(), InSituLogError> {
    let file = File::open(format!(
        "{}/testing/data/VuSitu_LiveReadings_2025-01-25_20-29-44_Device_Location.zip",
        env!["CARGO_MANIFEST_DIR"]
    ))?;
    let mut file = BufReader::new(&file);
    let log = InSituLogReader::from_zipped_html(&mut file)?;

    // Write attr to json file
    let mut json_file = File::create("vusitu.json")?;
    let json_str = serde_json::to_string_pretty(&log.attr).unwrap();
    json_file.write_all(json_str.as_bytes())?;

    // Wite log_note to csv file
    if let Some(log_note) = log.log_note {
        let log_note_csv_file = File::create("vusitu_note.csv")?;
        CsvWriter::new(log_note_csv_file).write(&log_note)?;
    }

    // Write log_data to csv file
    let log_data_csv_file = File::create("vusitu_data.csv")?;
    CsvWriter::new(log_data_csv_file).write(&log.log_data)?;

    Ok(())
}
