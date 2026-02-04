use std::io::{BufRead, Seek, SeekFrom};

use arrow::array::RecordBatch;
use serde_json::{Map, Value};
use unicode_segmentation::UnicodeSegmentation;

use crate::error::AquaTrollLogError;

use super::common::{DateTimeParser, TableBuilder};

#[derive(Debug)]
enum LineContent<'a> {
    Header(&'a str),
    Entry(&'a str, &'a str),
}

fn parse_line_content(line: &str) -> LineContent<'_> {
    let line_trim = line.trim();
    line_trim
        .split_once(":")
        .map(|(k, v)| (k.trim(), v.trim()))
        .map(|(k, v)| match v.is_empty() & !line.starts_with(" ") {
            true => LineContent::Header(k),
            false => LineContent::Entry(k, v),
        })
        .unwrap_or_else(|| LineContent::Header(line_trim))
}

/// Read general atttributs of the log file
pub(crate) fn read_attr<R: BufRead + Seek>(
    reader: &mut R,
    attr: &mut Map<String, Value>,
    is_root: bool,
) -> Result<(), AquaTrollLogError> {
    let mut buf = String::new();

    loop {
        buf.clear();
        let read_size = reader.read_line(&mut buf)?;

        // End of file
        if read_size == 0 {
            break;
        }

        let buf_trim = buf.trim();

        // Empty line
        if buf_trim.is_empty() {
            continue;
        }

        // Section break
        if buf_trim.chars().all(|c| c == '_') {
            if !is_root {
                reader.seek_relative(-(read_size as i64))?;
            }
            break;
        }

        match parse_line_content(&buf) {
            LineContent::Header(k) => {
                if is_root {
                    let mut new_block = Map::new();
                    read_attr(reader, &mut new_block, false)?;
                    attr.insert(k.to_string(), Value::Object(new_block));
                } else {
                    reader.seek_relative(-(read_size as i64))?;
                    break;
                }
            }
            LineContent::Entry(k, v) => {
                attr.insert(k.to_string(), Value::String(v.to_string()));
            }
        }
    }

    Ok(())
}

fn detect_column_span<R: BufRead>(
    reader: &mut R,
) -> Result<(usize, Vec<(usize, usize)>), AquaTrollLogError> {
    let mut spans = vec![];
    let mut line_offset = 0usize;

    let mut buf = String::new();
    loop {
        buf.clear();
        let read_size = reader.read_line(&mut buf)?;

        if read_size == 0 {
            return Err(AquaTrollLogError::UnexpectedEof);
        }

        let buf_trim = buf.trim();

        // Empty line
        if buf_trim.is_empty() {
            line_offset += 1;
            continue;
        }

        if buf_trim.chars().all(|c| c == '-' || c.is_whitespace()) {
            let mut range = (0, 0);
            for (n, c) in buf_trim.chars().enumerate() {
                if c == '-' {
                    if range.1 >= range.0 {
                        range.1 = n
                    } else {
                        range = (n, n)
                    }
                } else if range.1 > range.0 {
                    spans.push(range);
                    range = (n + 1, n)
                } else {
                    range = (n + 1, n)
                }
            }
            if range != (0, 0) {
                spans.push(range);
            }
            break;
        }

        line_offset += 1;
    }

    Ok((line_offset, spans))
}

/// Parse table data of the log file
pub(crate) fn read_table<R: BufRead + Seek>(
    reader: &mut R,
    datetime_parser: &DateTimeParser,
) -> Result<RecordBatch, AquaTrollLogError> {
    let mut buf = String::new();

    let start_pos = reader.stream_position()?; // Get current position of reader
    let (line_offset, col_ranges) = detect_column_span(reader)?;

    // Seek to line contains column names
    reader.seek(SeekFrom::Start(start_pos))?;
    for _ in 0..line_offset {
        buf.clear();
        reader.read_line(&mut buf)?;
    }

    let fields = col_ranges
        .iter()
        .map(|range| {
            buf[range.0..usize::min(range.1 + 1, buf.trim().len())]
                .trim()
                .to_string()
        })
        .collect();
    let mut table_builder = TableBuilder::new()
        .field_names(fields)
        .with_datetime_parser(datetime_parser.clone());

    let mut buf = String::new();
    reader.read_line(&mut buf)?;

    loop {
        buf.clear();
        let read_size = reader.read_line(&mut buf)?;

        // End of file
        if read_size == 0 {
            break;
        }

        let buf_trim = buf.trim();

        // Empty line
        if buf_trim.is_empty() {
            continue;
        }

        // Section break
        if buf_trim.chars().all(|c| c == '_') {
            break;
        }

        // A single `grapheme` may compose with multiple code points
        let buf_graphemes: Vec<&str> = buf_trim.graphemes(true).collect();
        let buf_len = buf_graphemes.len();

        let row = col_ranges
            .iter()
            .map(|&(l, r)| {
                buf_graphemes[l..usize::min(r + 1, buf_len)]
                    .concat()
                    .trim()
                    .to_string()
            })
            .collect();
        table_builder = table_builder.try_push_row(row)?;
    }

    table_builder.try_build()
}

pub(crate) fn read_log_data_attr<R: BufRead + Seek>(
    reader: &mut R,
) -> Result<Map<String, Value>, AquaTrollLogError> {
    let mut buf = String::new();

    // Skip until "Log Data:"
    loop {
        let read_size = reader.read_line(&mut buf)?;

        // End of file
        if read_size == 0 {
            break;
        }

        if buf.trim() == "Log Data:" {
            buf.clear();
            break;
        }
        buf.clear();
    }

    let mut log_data = Map::new();

    // Get record count
    let _ = reader.read_line(&mut buf)?;
    let record_count = if let LineContent::Entry(key, value) = parse_line_content(&buf) {
        if key != "Record Count" {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Expected 'Record Count' in Log Data",
            ))?
        }
        value.parse::<usize>()?
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Expected 'Record Count' in Log Data",
        ))?;
    };
    buf.clear();
    log_data.insert(
        "Record Count".to_string(),
        Value::Number(record_count.into()),
    );

    // Get sensor count
    let _ = reader.read_line(&mut buf)?;
    let sensor_count = if let LineContent::Entry(key, value) = parse_line_content(&buf) {
        if key != "Sensors" {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Expected 'Sensors' in Log Data",
            ))?
        }
        value.parse::<usize>()?
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Expected 'Sensors' in Log Data",
        ))?;
    };
    buf.clear();

    // Get sensors data
    let mut sensors = vec![];
    for n in 1..=sensor_count {
        let _ = reader.read_line(&mut buf)?;
        if let LineContent::Entry(key, value) = parse_line_content(&buf) {
            if key
                .split_whitespace()
                .next()
                .and_then(|k| k.parse::<usize>().ok())
                .ok_or(AquaTrollLogError::InvalidData)?
                != n
            {
                return Err(AquaTrollLogError::InvalidData);
            }
            let serial = key
                .split_whitespace()
                .last()
                .ok_or(AquaTrollLogError::InvalidData)?;
            let sensor = value;
            sensors.push((sensor.to_string(), serial.to_string()))
        } else {
            return Err(AquaTrollLogError::InvalidData);
        };

        buf.clear();
    }
    log_data.insert(
        "Sensors".to_string(),
        Value::Array(
            sensors
                .into_iter()
                .map(|(sensor, serial)| {
                    Value::Object(
                        vec![
                            ("Sensor".to_string(), Value::String(sensor)),
                            ("Serial".to_string(), Value::String(serial)),
                        ]
                        .into_iter()
                        .collect(),
                    )
                })
                .collect(),
        ),
    );

    // Get time zone
    let _ = reader.read_line(&mut buf)?;
    if let LineContent::Entry(key, value) = parse_line_content(&buf) {
        if key != "Time Zone" {
            return Err(AquaTrollLogError::InvalidData);
        }
        log_data.insert(key.to_string(), Value::String(value.to_string()));
    } else {
        return Err(AquaTrollLogError::InvalidData);
    };

    Ok(log_data)
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, str::FromStr};

    use serde_json::{json, Number};

    use super::*;

    static ATTR_TXT: &str = r#"
Report Date: 2025/1/2 PM 12:23:23
Report User Name: USER


Log File Properties:
                          File Name: sample.wsl
                        Create Date: 2025/1/1 PM 12:10:51

Device Properties:
                               Site: Sample Site
                        Device Name:  
                      Serial Number: 999996
                   Firmware Version: 2.37

Log Configuration
                      Computer Name: PC
                        Sample Rate: Days: 0 hrs: 00 mins: 00 secs: 15
                       High Trigger: 0 (pH)

Other Log Settings
                        Temperature: 21.4429 (C)

        Specific Conductivity Model: Standard Methods

                         TDS Factor: 0.65


______________________________________________________________________________________________________________
    "#;

    #[test]
    fn attr_parser() {
        let mut buf = Cursor::new(ATTR_TXT.as_bytes());
        let mut attr = Map::new();

        read_attr(&mut buf, &mut attr, true).unwrap();

        assert_eq!(
            serde_json::to_string(&attr).unwrap(),
            serde_json::to_string(&json!({
                "Report Date": "2025/1/2 PM 12:23:23",
                "Report User Name": "USER",
                "Log File Properties": {
                    "File Name": "sample.wsl",
                    "Create Date": "2025/1/1 PM 12:10:51"
                },
                "Device Properties": {
                    "Site": "Sample Site",
                    "Device Name": "",
                    "Serial Number": "999996",
                    "Firmware Version": "2.37"
                },
                "Log Configuration": {
                    "Computer Name": "PC",
                    "Sample Rate": "Days: 0 hrs: 00 mins: 00 secs: 15",
                    "High Trigger": "0 (pH)"
                },
                "Other Log Settings": {
                    "Temperature": "21.4429 (C)",
                    "Specific Conductivity Model": "Standard Methods",
                    "TDS Factor": "0.65"
                }
            }))
            .unwrap()
        );
    }

    static LOG_NOTE_TXT: &str = r#"
Log Notes:
Date and Time              Note
----------------------     -----------------------------------------------------------------------------------
2025/1/29 PM 04:00:21      Used Battery: 56% Used Memory: 26%   User Name: USER
2025/1/30 AM 07:16:58      Used Battery: 66% Used Memory: 29%   User Name: USER
2025/1/30 AM 07:16:58      Manual Stop Command
______________________________________________________________________________________________________________
    "#;

    #[test]
    fn log_note_parser() {
        let mut buf = Cursor::new(LOG_NOTE_TXT.as_bytes());
        let notes = read_table(&mut buf, &DateTimeParser::Default).unwrap();
        assert_eq!(notes.num_columns(), 2);
        assert_eq!(notes.num_rows(), 3);
        assert_eq!(notes.schema().field(0).name(), "DateTime");
        assert_eq!(notes.schema().field(1).name(), "Note");
    }

    static LOG_DATA_TXT: &str = r#"
Log Data:
Record Count: 2
Sensors: 6
	1 - 999991: pH/ORP
	2 - 999995: Rugged Dissolved Oxygen (RDO)
	3 - 999997: Conductivity
	4 - 999999: Turbidity
	5 - 999996: Internal
	6 - 999998: Pressure (200m/650ft)
Time Zone: 台北標準時間

                                            Sensor: pH/ORP                               Sensor: pH/ORP                               Sensor: pH/ORP                               Sensor: RDO                                  Sensor: RDO                                  Sensor: RDO                                  Sensor: Cond                                 Sensor: Cond                                 Sensor: Cond                                 Sensor: Cond                                 Sensor: Cond                                 Sensor: Cond                                 Sensor: Cond                                 Sensor: Turb                                 Sensor: Internal                             Sensor: Internal                             Sensor: Internal                             Sensor: Baro                                 Sensor: Pres 650ft                           Sensor: Pres 650ft                           
                           Elapsed Time     SN#: 999991                                  SN#: 999991                                  SN#: 999991                                  SN#: 999995                                  SN#: 999995                                  SN#: 999995                                  SN#: 999997                                  SN#: 999997                                  SN#: 999997                                  SN#: 999997                                  SN#: 999997                                  SN#: 999997                                  SN#: 999997                                  SN#: 999999                                 SN#: 999996                                  SN#: 999996                                  SN#: 999996                                  SN#: 999996                                  SN#: 999998                                  SN#: 999998                                  
Date and Time              Seconds          pH (pH)                                      pH(mV) (mV)                                  Oxidation Reduction Potential (ORP) (mV)     Dissolved Oxygen (concentration) (mg/L)      Dissolved Oxygen (%saturation) (%Sat)        Partial Pressure Oxygen (Torr)               Temperature (C)                              Actual Conductivity (µS/cm)                  Specific Conductivity (µS/cm)                Salinity (PSU)                               Resistivity (ohm-cm)                         Water Density (g/cm3)                        Total Dissolved Solids (ppm)                 Turbidity (NTU)                              Temperature (C)                              External Voltage (V)                         Battery Percentage (%)                       Barometric Pressure (mmHg)                   Pressure (PSI)                               Depth (m)                                    
----------------------     ------------     ----------------------------------------     ----------------------------------------     ----------------------------------------     ----------------------------------------     ----------------------------------------     ----------------------------------------     ----------------------------------------     ----------------------------------------     ----------------------------------------     ----------------------------------------     ----------------------------------------     ----------------------------------------     ----------------------------------------     ----------------------------------------     ----------------------------------------     ----------------------------------------     ----------------------------------------     ----------------------------------------     ----------------------------------------     ----------------------------------------     
2025/1/30 PM 05:00:59             0.000                                        7.736                                      -39.768                                      131.525                                        1.393                                       15.362                                       21.540                                       21.444                                      271.551                                      291.341                                        0.140                                     3682.546                                        0.998                                      189.372                                       48.264                                       21.444                                        0.198                                       43.000                                      780.048                                       14.524                                       10.317     
2025/1/30 PM 05:01:14            15.000                                        7.736                                      -39.768                                      131.525                                        1.393                                       15.362                                       21.540                                       21.444                                      271.551                                      291.341                                        0.140                                     3682.546                                        0.998                                      189.372                                       48.264                                       21.444                                        0.198                                       43.000                                      780.048                                       14.524                                       10.317     
    "#;

    #[test]
    fn log_data_attr() {
        let mut buf = Cursor::new(LOG_DATA_TXT.as_bytes());
        let data_attr = read_log_data_attr(&mut buf).unwrap();

        assert!(match &data_attr["Record Count"] {
            Value::Number(n) => n == &Number::from_str("2").unwrap(),
            _ => false,
        });
        assert!(match &data_attr["Sensors"] {
            Value::Array(ar) => ar.len() == 6,
            _ => false,
        })
    }

    #[test]
    fn log_data_table() {
        let mut buf = Cursor::new(LOG_DATA_TXT.as_bytes());
        let data_table = read_table(&mut buf, &DateTimeParser::Default).unwrap();

        assert_eq!(data_table.num_columns(), 22);
        assert_eq!(data_table.schema().field(0).name(), "DateTime");
        assert_eq!(data_table.schema().field(2).name(), "pH (pH)");
    }
}
