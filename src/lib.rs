mod error;
mod util;

use std::io::{Cursor, Read, Seek};

use arrow::array::RecordBatch;
use encoding_rs::{ISO_8859_3, UTF_16LE};
use encoding_rs_io::DecodeReaderBytesBuilder;
pub use error::AquaTrollLogError;
use serde::Serialize;
use serde_json::{Map, Value};
use util::{
    common::record_batch_to_json, read_attr, read_csv_table, read_html, read_log_data_attr,
    read_table, read_zipped_html,
};

#[derive(Debug)]
pub struct AquaTrollLogReader {
    pub attr: Map<String, Value>,
    pub log_note: Option<RecordBatch>,
    pub log_data: RecordBatch,
}

impl AquaTrollLogReader {
    // TODO: Add troll calibration file reader
    // TODO: Check and convert unit of table data by numbat

    pub fn from_csv<R: Read + Seek>(reader: &mut R) -> Result<Self, AquaTrollLogError> {
        let mut decode = DecodeReaderBytesBuilder::new()
            .encoding(Some(ISO_8859_3))
            .build(reader);
        let mut buf = Vec::new();
        let _ = decode.read_to_end(&mut buf)?;
        let mut reader = Cursor::new(buf);

        Ok(Self {
            attr: Map::new(),
            log_note: None,
            log_data: read_csv_table(&mut reader)?,
        })
    }

    pub fn from_txt<R: Read + Seek>(reader: &mut R) -> Result<Self, AquaTrollLogError> {
        // The exported txt log file from WinSitu is encodeded with UTF-16LE.
        let mut decode = DecodeReaderBytesBuilder::new()
            .encoding(Some(UTF_16LE))
            .build(reader);
        let mut buf = Vec::new();
        let _ = decode.read_to_end(&mut buf)?;
        let mut reader = Cursor::new(buf);

        let mut attr = Map::new();
        read_attr(&mut reader, &mut attr, true)?;
        let log_note = read_table(&mut reader)?;
        let log_data_attr = read_log_data_attr(&mut reader)?;
        attr.insert("Log Data".to_string(), Value::Object(log_data_attr));
        let log_data = read_table(&mut reader)?;

        Ok(Self {
            attr,
            log_note: Some(log_note),
            log_data,
        })
    }

    pub fn from_html<R: Read>(reader: &mut R) -> Result<Self, AquaTrollLogError> {
        let (attr, log_data) = read_html(reader)?;

        Ok(Self {
            attr,
            log_note: None,
            log_data,
        })
    }

    pub fn from_zipped_html<R: Read + Seek>(reader: &mut R) -> Result<Self, AquaTrollLogError> {
        let (attr, log_data) = read_zipped_html(reader)?;

        Ok(Self {
            attr,
            log_note: None,
            log_data,
        })
    }

    pub fn to_json(&self) -> Result<Value, AquaTrollLogError> {
        let mut json_object = Map::new();

        json_object.insert("attr".to_string(), Value::Object(self.attr.clone()));
        json_object.insert(
            "log_note".to_string(),
            if self.log_note.is_some() {
                record_batch_to_json(self.log_note.as_ref().unwrap())?
            } else {
                Value::Null
            },
        );
        json_object.insert(
            "log_data".to_string(),
            record_batch_to_json(&self.log_data)?,
        );

        Ok(Value::Object(json_object))
    }
}

impl Serialize for AquaTrollLogReader {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_json()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}
