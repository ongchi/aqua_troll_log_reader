use std::io::{BufRead, Seek};

use arrow::array::RecordBatch;
use csv::ErrorKind;
use csv::StringRecord;

use crate::error::AquaTrollLogError;

use super::common::TableBuilder;

#[derive(thiserror::Error, Debug)]
pub struct CsvErrorWithPartialResult {
    pub(crate) result: RecordBatch,
    pub(crate) errors: Vec<csv::Error>,
}

impl std::fmt::Display for CsvErrorWithPartialResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "CSV Error with partial result:")?;
        for e in &self.errors {
            writeln!(f, "{e}")?;
        }

        Ok(())
    }
}

/// Read csv log data
pub(crate) fn read_table<R: BufRead + Seek>(
    reader: &mut R,
) -> Result<RecordBatch, AquaTrollLogError> {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(true);
    let mut csv_reader = builder.from_reader(reader);

    let fields: Vec<String> = {
        let headers = &csv_reader.headers().unwrap().clone();
        headers.iter().map(|s| s.to_string()).collect()
    };
    let fields_len = fields.len();

    let mut table_builder = TableBuilder::new().field_names(fields.clone());
    let mut record = StringRecord::new();

    let mut csv_errors: Vec<csv::Error> = vec![];

    loop {
        match csv_reader.read_record(&mut record) {
            Ok(next) => {
                let values: Vec<String> = record.iter().map(|v| v.to_string()).collect();
                if values.len() == fields_len
                    && !fields.iter().zip(values.iter()).any(|(f, v)| f == v)
                {
                    table_builder = table_builder.try_push_row(values)?;
                };
                if !next {
                    break;
                }
            }
            Err(e) => match e.kind() {
                // Skip invalid rows
                ErrorKind::UnequalLengths { .. } => {
                    csv_errors.push(e);
                    continue;
                }
                _ => return Err(AquaTrollLogError::from(e)),
            },
        }
    }

    if csv_errors.is_empty() {
        table_builder.try_build()
    } else {
        Err(AquaTrollLogError::WithCsvPartialResult(
            CsvErrorWithPartialResult {
                result: table_builder.try_build()?,
                errors: csv_errors,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    static LOG_DATA_CSV: &str = r#"Date/Time,Temp(C),CNDCT(µS/cm),SPCNDCT(µS/cm),R(ohm-cm),SA(PSU),TDS(ppm),pH(pH),ORP(mV),DO(con)(mg/L),DO(%sat)(%Sat)
2025/1/25 05:15:06 PM,21.6019,416.245,445.136,2402.43,0.216156,289.339,7.40582,173.966,5.43175,56.0774
2025/1/25 05:15:36 PM,21.6097,416.924,445.791,2398.52,0.216483,289.764,7.40086,172.221,5.33604,55.0975
2025/1/25 05:16:06 PM,21.6239,416.77,445.497,2399.41,0.216336,289.573,7.40294,169.584,5.23239,54.0421
2025/1/25 05:16:36 PM,21.6365,416.756,445.368,2399.49,0.216272,289.489,7.40594,166.954,5.14173,53.1185
2025/1/25 05:17:06 PM,21.6499,416.724,445.211,2399.67,0.216194,289.387,7.40294,165.011,5.04762,52.1598
2025/1/25 05:17:36 PM,21.6602,416.722,445.117,2399.68,0.216147,289.326,7.401,162.434,4.96579,51.3241
2025/1/25 05:18:06 PM,21.6709,416.815,445.118,2399.15,0.216148,289.327,7.39776,160.219,4.89553,50.6084
2025/1/25 05:18:36 PM,21.6804,416.867,445.088,2398.85,0.216133,289.307,7.40554,156.566,4.82426,49.8807
"#;

    #[test]
    fn test_read_table() {
        let mut reader = Cursor::new(LOG_DATA_CSV);
        let data_table = read_table(&mut reader).unwrap();
        assert_eq!(
            data_table
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<&String>>(),
            vec![
                "Date/Time",
                "Temp(C)",
                "CNDCT(µS/cm)",
                "SPCNDCT(µS/cm)",
                "R(ohm-cm)",
                "SA(PSU)",
                "TDS(ppm)",
                "pH(pH)",
                "ORP(mV)",
                "DO(con)(mg/L)",
                "DO(%sat)(%Sat)"
            ]
        );
        assert_eq!(data_table.num_rows(), 8);
    }

    static LOG_DATA_MULTIPLE_HEADERS_CSV: &str = r#"Date/Time,Temp(C),CNDCT(µS/cm),SPCNDCT(µS/cm),R(ohm-cm),SA(PSU),TDS(ppm),pH(pH),ORP(mV),DO(con)(mg/L),DO(%sat)(%Sat)
2025/1/25 05:15:06 PM,21.6019,416.245,445.136,2402.43,0.216156,289.339,7.40582,173.966,5.43175,56.0774
2025/1/25 05:15:36 PM,21.6097,416.924,445.791,2398.52,0.216483,289.764,7.40086,172.221,5.33604,55.0975
2025/1/25 05:16:06 PM,21.6239,416.77,445.497,2399.41,0.216336,289.573,7.40294,169.584,5.23239,54.0421
Date/Time,Temp(C),CNDCT(µS/cm),SPCNDCT(µS/cm),R(ohm-cm),SA(PSU),TDS(ppm),pH(pH),ORP(mV),DO(con)(mg/L),DO(%sat)(%Sat)
2025/1/25 05:16:36 PM,21.6365,416.756,445.368,2399.49,0.216272,289.489,7.40594,166.954,5.14173,53.1185
2025/1/25 05:17:06 PM,21.6499,416.724,445.211,2399.67,0.216194,289.387,7.40294,165.011,5.04762,52.1598
Date/Time,Temp(C),CNDCT(µS/cm),SPCNDCT(µS/cm),R(ohm-cm),SA(PSU),TDS(ppm),pH(pH),ORP(mV),DO(con)(mg/L),DO(%sat)(%Sat)
2025/1/25 05:17:36 PM,21.6602,416.722,445.117,2399.68,0.216147,289.326,7.401,162.434,4.96579,51.3241
"#;

    #[test]
    fn test_read_multiple_headers_table() {
        let mut reader = Cursor::new(LOG_DATA_MULTIPLE_HEADERS_CSV);
        let data_table = read_table(&mut reader).unwrap();
        assert_eq!(data_table.num_rows(), 6);
    }

    static LOG_DATA_INCOMPLETE_CSV: &str = r#"Date/Time,Temp(C),CNDCT(µS/cm),SPCNDCT(µS/cm),R(ohm-cm),SA(PSU),TDS(ppm),pH(pH),ORP(mV),DO(con)(mg/L),DO(%sat)(%Sat)
2025/1/25 05:15:06 PM,21.6019,416.245,445.136,2402.43,0.216156,289.339,7.40582,173.966,5.43175,56.0774
2025/1/25 05:15:36 PM,21.6097,416.924,445.791,2398.52,0.216483,289.764,7.40086,172.221,5.33604,55.0975
2025/1/25 05:16:06 PM,21.6239,416.77,445.497,2399.41,0.216336,289.573,7.40294,169.58
Date/Time,Temp(C),CNDCT(µS/cm),SPCNDCT(µS/cm),R(ohm-cm),SA(PSU),TDS(ppm),pH(pH),ORP(mV),DO(con)(mg/L),DO(%sat)(%Sat)
2025/1/25 05:16:36 PM,21.6365,416.756,445.368,2399.49,0.216272,289.489,7.40594,166.954,5.14173,53.1185
2025/1/25 05:17:06 PM,21.6499,416.724,445.211,2399.67,0.216194,289.387,7.40294,165.011,5.04762,52.1598
"#;

    #[test]
    fn test_read_incomplete_table() {
        let mut reader = Cursor::new(LOG_DATA_INCOMPLETE_CSV);
        let data_table = match read_table(&mut reader) {
            Err(AquaTrollLogError::WithCsvPartialResult(partial_result)) => partial_result.result,
            _ => panic!("Expected a CSV error with partial result"),
        };
        assert_eq!(data_table.num_rows(), 4);
    }
}
