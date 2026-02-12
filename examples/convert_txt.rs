use std::{fs::File, io::Write};

use aqua_troll_log_reader::{AquaTrollLogError, AquaTrollLogReader};

// Convert log file to json and csv format
fn main() -> Result<(), AquaTrollLogError> {
    let mut file = File::open(format!(
        "{}/testing/data/win_situ_dump.txt",
        env!["CARGO_MANIFEST_DIR"]
    ))?;

    let reader = AquaTrollLogReader::default();
    let log = reader.read_txt(&mut file)?;

    // Write attr to json file
    let mut json_file = File::create("ex_txt_attr.json")?;
    let json_str = serde_json::to_string_pretty(&log.attr).unwrap();
    json_file.write_all(json_str.as_bytes())?;

    // Write log_note to csv file
    if let Some(ref log_note) = log.log_note {
        let log_note_csv_file = File::create("ex_txt_note.csv")?;
        log_note.write_csv(log_note_csv_file)?;
    }

    // Write log_data to csv file
    let log_data_csv_file = File::create("ex_txt_data.csv")?;
    log.log_data.write_csv(log_data_csv_file)?;

    Ok(())
}
