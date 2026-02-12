use std::{
    fs::File,
    io::{BufReader, Write},
};

use aqua_troll_log_reader::{AquaTrollLogError, AquaTrollLogReader};

// convert zipped html log file to json and csv files
fn main() -> Result<(), AquaTrollLogError> {
    let file = File::open(format!(
        "{}/testing/data/VuSitu_LiveReadings_2025-01-25_20-29-44_Device_Location.zip",
        env!["CARGO_MANIFEST_DIR"]
    ))?;
    let mut file = BufReader::new(&file);
    let reader = AquaTrollLogReader::default();
    let log = reader.read_zipped_html(&mut file)?;

    // Write attr to json file
    let mut json_file = File::create("ex_html_attr.json")?;
    let json_str = serde_json::to_string_pretty(&log.attr).unwrap();
    json_file.write_all(json_str.as_bytes())?;

    // Write log_note to csv file
    if let Some(ref log_note) = log.log_note {
        let log_note_csv_file = File::create("ex_html_note.csv")?;
        log_note.write_csv(log_note_csv_file)?;
    }

    // Write log_data to csv file
    let log_data_csv_file = File::create("ex_html_data.csv")?;
    log.log_data.write_csv(log_data_csv_file)?;

    Ok(())
}
