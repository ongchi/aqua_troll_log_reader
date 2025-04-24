mod error;
mod util;

use std::io::{Cursor, Read, Seek};

use arrow::array::RecordBatch;
use encoding_rs::{ISO_8859_3, UTF_16LE};
use encoding_rs_io::DecodeReaderBytesBuilder;
pub use error::InSituLogError;
use serde_json::{Map, Value};
use util::{
    read_attr, read_csv_table, read_html, read_log_data_attr, read_table, read_zipped_html,
};

#[derive(Debug)]
pub struct InSituLogReader {
    pub attr: Map<String, Value>,
    pub log_note: Option<RecordBatch>,
    pub log_data: RecordBatch,
}

impl InSituLogReader {
    // TODO: Add troll calibration file reader
    // TODO: Check and convert unit of table data by numbat

    pub fn from_csv<R: Read + Seek>(reader: &mut R) -> Result<Self, InSituLogError> {
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

    pub fn from_txt<R: Read + Seek>(reader: &mut R) -> Result<Self, InSituLogError> {
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

    pub fn from_html<R: Read>(reader: &mut R) -> Result<Self, InSituLogError> {
        let (attr, log_data) = read_html(reader)?;

        Ok(Self {
            attr,
            log_note: None,
            log_data,
        })
    }

    pub fn from_zipped_html<R: Read + Seek>(reader: &mut R) -> Result<Self, InSituLogError> {
        let (attr, log_data) = read_zipped_html(reader)?;

        Ok(Self {
            attr,
            log_note: None,
            log_data,
        })
    }
}
