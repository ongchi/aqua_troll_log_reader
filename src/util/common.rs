use std::rc::Rc;

use chrono::NaiveDateTime;
use serde::ser::SerializeSeq;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::error::AquaTrollLogError;

pub(crate) fn parse_datetime_str(datetime: &str) -> Result<NaiveDateTime, AquaTrollLogError> {
    Ok(
        NaiveDateTime::parse_from_str(datetime, "%Y/%-m/%-d %p %I:%M:%S")
            .or_else(|_| NaiveDateTime::parse_from_str(datetime, "%Y/%-m/%-d %I:%M:%S %p"))
            .or_else(|_| NaiveDateTime::parse_from_str(datetime, "%Y-%-m-%-d %H:%M:%S"))?,
    )
}

pub(crate) fn parse_datetime_with_format(
    datetime: &str,
    format: &str,
) -> Result<NaiveDateTime, AquaTrollLogError> {
    NaiveDateTime::parse_from_str(datetime, format).map_err(Into::into)
}

pub type DateTimeParserFnRef = Rc<dyn Fn(&str) -> Result<NaiveDateTime, AquaTrollLogError>>;
#[derive(Clone)]
pub struct DateTimeParserFn(DateTimeParserFnRef);

impl std::fmt::Debug for DateTimeParserFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<DateTime Parser Function>")
    }
}

#[derive(Default, Debug, Clone)]
pub enum DateTimeParser {
    #[default]
    Default,
    Format(String),
    Custom(DateTimeParserFn),
}

impl DateTimeParser {
    pub fn parse(&self, datetime_str: &str) -> Result<NaiveDateTime, AquaTrollLogError> {
        match self {
            DateTimeParser::Default => parse_datetime_str(datetime_str),
            DateTimeParser::Format(fmt) => parse_datetime_with_format(datetime_str, fmt),
            DateTimeParser::Custom(f) => f.0(datetime_str),
        }
    }
}

impl From<&str> for DateTimeParser {
    fn from(value: &str) -> Self {
        DateTimeParser::Format(value.to_string())
    }
}

impl From<DateTimeParserFnRef> for DateTimeParser {
    fn from(value: DateTimeParserFnRef) -> Self {
        DateTimeParser::Custom(DateTimeParserFn(value))
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum CellValue {
    DateTime(NaiveDateTime),
    Float64(f64),
    Text(String),
}

impl std::fmt::Display for CellValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CellValue::DateTime(dt) => write!(f, "{}", dt.format("%Y-%m-%d %H:%M:%S")),
            CellValue::Float64(v) => write!(f, "{v}"),
            CellValue::Text(s) => write!(f, "{s}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Table {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<CellValue>>,
}

impl Table {
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    pub fn column_name(&self, index: usize) -> &str {
        &self.columns[index]
    }

    /// Write the table as CSV to any `io::Write` destination.
    pub fn write_csv<W: std::io::Write>(&self, writer: W) -> Result<(), csv::Error> {
        let mut csv_writer = csv::Writer::from_writer(writer);
        csv_writer.write_record(&self.columns)?;
        for row in &self.rows {
            let fields: Vec<String> = row.iter().map(|v| v.to_string()).collect();
            csv_writer.write_record(&fields)?;
        }
        csv_writer.flush()?;
        Ok(())
    }
}

impl Serialize for Table {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.rows.len()))?;
        for row in &self.rows {
            let obj: Map<String, Value> = self
                .columns
                .iter()
                .zip(row.iter())
                .map(|(col, val)| {
                    let v = match val {
                        CellValue::DateTime(dt) => {
                            Value::String(dt.format("%Y-%m-%dT%H:%M:%S").to_string())
                        }
                        CellValue::Float64(f) => serde_json::Number::from_f64(*f)
                            .map(Value::Number)
                            .unwrap_or(Value::Null),
                        CellValue::Text(s) => Value::String(s.clone()),
                    };
                    (col.clone(), v)
                })
                .collect();
            seq.serialize_element(&obj)?;
        }
        seq.end()
    }
}

#[derive(Clone, Copy)]
enum ColumnType {
    DateTime,
    Text,
    Float64,
}

pub(crate) struct TableBuilder {
    column_types: Vec<ColumnType>,
    columns: Vec<String>,
    rows: Vec<Vec<CellValue>>,
    datetime_parser: DateTimeParser,
}

impl TableBuilder {
    pub fn new() -> Self {
        Self {
            column_types: Vec::new(),
            columns: Vec::new(),
            rows: Vec::new(),
            datetime_parser: DateTimeParser::Default,
        }
    }

    pub fn field_names(mut self, field_names: Vec<String>) -> Self {
        let mut columns = Vec::new();
        let mut column_types = Vec::new();

        for name in field_names {
            if ["Date and Time", "Date Time", "Date/Time", "DateTime"].contains(&name.as_str()) {
                columns.push("DateTime".to_string());
                column_types.push(ColumnType::DateTime);
            } else if name == "Note" || name == "Marked" {
                columns.push(name);
                column_types.push(ColumnType::Text);
            } else {
                columns.push(name);
                column_types.push(ColumnType::Float64);
            }
        }

        self.columns = columns;
        self.column_types = column_types;
        self
    }

    pub fn with_datetime_parser(mut self, parser: DateTimeParser) -> Self {
        self.datetime_parser = parser;
        self
    }

    pub fn try_push_row(mut self, row_values: Vec<String>) -> Result<Self, AquaTrollLogError> {
        let mut row = Vec::with_capacity(row_values.len());
        for (value_str, col_type) in row_values.into_iter().zip(&self.column_types) {
            let cell = match col_type {
                ColumnType::DateTime => {
                    CellValue::DateTime(self.datetime_parser.parse(&value_str)?)
                }
                ColumnType::Text => CellValue::Text(value_str),
                ColumnType::Float64 => CellValue::Float64(value_str.parse()?),
            };
            row.push(cell);
        }
        self.rows.push(row);
        Ok(self)
    }

    pub fn try_build(self) -> Result<Table, AquaTrollLogError> {
        if self.columns.is_empty() {
            return Err(AquaTrollLogError::InvalidData);
        }
        Ok(Table {
            columns: self.columns,
            rows: self.rows,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datetime_str() {
        let datetime = "2021/7/20 PM 12:00:00";
        let result = parse_datetime_str(datetime).unwrap();
        assert_eq!(
            result,
            NaiveDateTime::parse_from_str("2021-07-20 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
        );
    }

    #[test]
    fn parse_with_custom_format() {
        let result =
            parse_datetime_with_format("2021-07-20 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        assert_eq!(
            result,
            NaiveDateTime::parse_from_str("2021-07-20 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
        );
    }

    #[test]
    fn parse_with_custom_format_alternative() {
        let result =
            parse_datetime_with_format("20/07/2021 12:00:00", "%d/%m/%Y %H:%M:%S").unwrap();
        assert_eq!(
            result,
            NaiveDateTime::parse_from_str("2021-07-20 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
        );
    }

    #[test]
    fn table_builder_with_datetime_format() {
        let field_names = vec!["Date Time".to_string(), "Value".to_string()];
        let table_builder = TableBuilder::new()
            .field_names(field_names)
            .with_datetime_parser("%Y-%m-%d %H:%M:%S".into());

        let row_values = vec!["2021-07-20 12:00:00".to_string(), "1.0".to_string()];
        let table_builder = table_builder.try_push_row(row_values).unwrap();

        let table = table_builder.try_build().unwrap();
        assert_eq!(table.num_rows(), 1);
    }

    #[test]
    fn table_builder_with_custom_parser() {
        let parser = Rc::new(|s: &str| -> Result<NaiveDateTime, AquaTrollLogError> {
            parse_datetime_with_format(s, "%d/%m/%Y %H:%M:%S")
        }) as DateTimeParserFnRef;

        let field_names = vec!["Date Time".to_string(), "Value".to_string()];
        let table_builder = TableBuilder::new()
            .field_names(field_names)
            .with_datetime_parser(parser.into());

        let row_values = vec!["20/07/2021 12:00:00".to_string(), "1.0".to_string()];
        let table_builder = table_builder.try_push_row(row_values).unwrap();

        let table = table_builder.try_build().unwrap();
        assert_eq!(table.num_rows(), 1);
    }

    #[test]
    fn table_builder_default_parser_unchanged() {
        let field_names = vec!["Date and Time".to_string(), "Value".to_string()];
        let table_builder = TableBuilder::new().field_names(field_names);

        let row_values = vec!["2021/7/20 PM 12:00:00".to_string(), "1.0".to_string()];
        let table_builder = table_builder.try_push_row(row_values).unwrap();

        let table = table_builder.try_build().unwrap();
        assert_eq!(table.num_rows(), 1);
    }

    #[test]
    fn table_builder_with_multiple_rows_custom_format() {
        let field_names = vec!["Date Time".to_string(), "Value".to_string()];
        let table_builder = TableBuilder::new()
            .field_names(field_names)
            .with_datetime_parser("%Y-%m-%d %H:%M:%S".into());

        let table_builder = table_builder
            .try_push_row(vec!["2021-07-20 12:00:00".to_string(), "1.0".to_string()])
            .unwrap()
            .try_push_row(vec!["2021-07-20 12:01:00".to_string(), "2.0".to_string()])
            .unwrap();

        let table = table_builder.try_build().unwrap();
        assert_eq!(table.num_rows(), 2);
    }

    #[test]
    fn table_builder() {
        let field_names = vec![
            "Date and Time".to_string(),
            "Note".to_string(),
            "Marked".to_string(),
            "Value".to_string(),
        ];
        let table_builder = TableBuilder::new().field_names(field_names);

        let row_values = vec![
            "2021/7/20 PM 12:00:00".to_string(),
            "Foo".to_string(),
            "Unmarked".to_string(),
            "1.0".to_string(),
        ];
        let table_builder = table_builder.try_push_row(row_values).unwrap();
        let row_values = vec![
            "2021/7/20 PM 12:01:00".to_string(),
            "Bar".to_string(),
            "Marked".to_string(),
            "2.0".to_string(),
        ];
        let table_builder = table_builder.try_push_row(row_values).unwrap();
        let table = table_builder.try_build().unwrap();

        assert_eq!(table.num_columns(), 4);
        assert_eq!(table.column_name(0), "DateTime");
        assert_eq!(table.column_name(1), "Note");
        assert_eq!(table.column_name(3), "Value");

        assert_eq!(table.num_rows(), 2);
        assert!(matches!(&table.rows[0][1], CellValue::Text(s) if s == "Foo"));
        assert!(matches!(&table.rows[1][1], CellValue::Text(s) if s == "Bar"));
        assert!(matches!(&table.rows[0][3], CellValue::Float64(v) if *v == 1.0));
        assert!(matches!(&table.rows[1][3], CellValue::Float64(v) if *v == 2.0));
    }
}
