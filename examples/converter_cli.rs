use std::fs::File;
use std::path::Path;
use std::rc::Rc;
use std::{env, sync::Arc};

use arrow::array::{Array, StringArray};
use arrow::csv::Writer as CsvWriter;
use arrow_schema::{DataType, Field, Schema};
use chrono::{Local, NaiveDateTime, TimeZone, Utc};

use aqua_troll_log_reader::{AquaTrollLogError, AquaTrollLogReader, DateTimeParserFnRef};

pub fn datetime_str_parser(datetime: &str) -> Result<i64, AquaTrollLogError> {
    let offset = *Local::now().offset();

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
            .or_else(|_| NaiveDateTime::parse_from_str(&datetime, "%Y-%-m-%-d %H:%M:%S"))
            .map(|t| t.and_local_timezone(offset).unwrap())
            .map(|t| t.timestamp())?,
    )
}

fn main() -> Result<(), AquaTrollLogError> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        println!("Usage: converter_cli <input.txt>");
        return Ok(());
    }

    let input = &args[1];

    let mut file = File::open(input)?;
    let datetime_parser = Rc::new(datetime_str_parser) as DateTimeParserFnRef;
    let log_reader = AquaTrollLogReader::new(datetime_parser.into());
    let log = log_reader.read_txt(&mut file)?;

    let path = Path::new(input);
    if let Some(filename) = path.file_stem() {
        if let Some(output) = filename.to_str() {
            // Write log_data to csv file
            let log_data_csv_file = File::create(format!("{output}.csv"))?;

            let schema = log.log_data.schema();
            let mut new_fields = vec![];
            for field in schema.fields().iter() {
                if field.name() == "DateTime" {
                    let new_field = Field::new("DateTime", DataType::Utf8, field.is_nullable());
                    new_fields.push(Arc::new(new_field));
                } else {
                    new_fields.push(field.clone());
                }
            }
            let new_schema = Arc::new(Schema::new(new_fields));
            let datetime_index = schema.index_of("DateTime")?;

            let mut new_columns: Vec<Arc<dyn Array>> = vec![];
            for (col_idx, column) in log.log_data.columns().iter().enumerate() {
                if col_idx == datetime_index {
                    let new_column = column
                        .as_any()
                        .downcast_ref::<arrow::array::TimestampSecondArray>()
                        .unwrap()
                        .clone();
                    let new_column = Arc::new(StringArray::from(
                        new_column
                            .values()
                            .into_iter()
                            .map(|t| Utc.timestamp_opt(*t, 0).unwrap())
                            .map(|t| t.with_timezone(&Local))
                            .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
                            .collect::<Vec<String>>(),
                    ));
                    new_columns.push(new_column);
                } else {
                    new_columns.push(column.clone());
                }
            }

            let new_record_batch =
                arrow::record_batch::RecordBatch::try_new(new_schema, new_columns)?;

            CsvWriter::new(log_data_csv_file).write(&new_record_batch)?;
        } else {
            println!("Invalid input file name");
        }
    } else {
        println!("Invalid input file");
    }

    Ok(())
}
