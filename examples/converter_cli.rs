use std::env;
use std::fs::File;
use std::path::Path;
use std::rc::Rc;

use chrono::NaiveDateTime;

use aqua_troll_log_reader::{AquaTrollLogError, AquaTrollLogReader, DateTimeParserFnRef};

fn datetime_str_parser(datetime: &str) -> Result<NaiveDateTime, AquaTrollLogError> {
    let datetime = if datetime.contains("上午") {
        datetime.replace("上午", "AM")
    } else if datetime.contains("下午") {
        datetime.replace("下午", "PM")
    } else {
        datetime.to_string()
    };

    Ok(
        NaiveDateTime::parse_from_str(&datetime, "%Y/%-m/%-d %p %I:%M:%S")
            .or_else(|_| NaiveDateTime::parse_from_str(&datetime, "%Y/%-m/%-d %I:%M:%S %p"))
            .or_else(|_| NaiveDateTime::parse_from_str(&datetime, "%Y-%-m-%-d %H:%M:%S"))?,
    )
}

fn main() -> Result<(), AquaTrollLogError> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        println!("Usage: converter_cli <input.txt>");
        return Ok(());
    }

    let input = &args[1];
    let path = Path::new(input);

    let Some(output) = path.file_stem().and_then(|s| s.to_str()) else {
        println!("Invalid input file name");
        return Ok(());
    };

    let mut file = File::open(input)?;
    let datetime_parser = Rc::new(datetime_str_parser) as DateTimeParserFnRef;
    let log_reader = AquaTrollLogReader::new(datetime_parser.into());
    let log = log_reader.read_txt(&mut file)?;

    // Write log_data to csv file
    let log_data_csv_file = File::create(format!("{output}.csv"))?;
    log.log_data.write_csv(log_data_csv_file)?;

    Ok(())
}
