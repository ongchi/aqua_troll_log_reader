use std::rc::Rc;
use std::sync::Arc;

use arrow::json::ArrayWriter;
use arrow::{
    array::{ArrayRef, GenericStringBuilder, PrimitiveBuilder, RecordBatch},
    datatypes::{DataType, Field, Float64Type, Schema, SchemaRef, TimeUnit, TimestampSecondType},
};
use chrono::{Local, NaiveDateTime};
use serde_json::Value;

use crate::error::AquaTrollLogError;

pub(crate) fn parse_datetime_str(datetime: &str) -> Result<i64, AquaTrollLogError> {
    let offset = *Local::now().offset();
    Ok(
        NaiveDateTime::parse_from_str(datetime, "%Y/%-m/%-d %p %I:%M:%S")
            .or_else(|_| NaiveDateTime::parse_from_str(datetime, "%Y/%-m/%-d %I:%M:%S %p"))
            .or_else(|_| NaiveDateTime::parse_from_str(datetime, "%Y-%-m-%-d %H:%M:%S"))
            .map(|t| t.and_local_timezone(offset).unwrap())
            .map(|t| t.timestamp())?,
    )
}

#[allow(dead_code)]
pub(crate) fn parse_datetime_with_format(
    datetime: &str,
    format: &str,
) -> Result<i64, AquaTrollLogError> {
    let offset = *Local::now().offset();
    Ok(NaiveDateTime::parse_from_str(datetime, format)
        .map(|t| t.and_local_timezone(offset).unwrap())
        .map(|t| t.timestamp())?)
}

pub type DateTimeParserFnRef = Rc<dyn Fn(&str) -> Result<i64, AquaTrollLogError>>;
#[derive(Clone)]
pub struct DateTimeParserFn(DateTimeParserFnRef);

impl std::fmt::Debug for DateTimeParserFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<DateTime Parser Function>")
    }
}

#[derive(Default, Debug, Clone)]
#[allow(dead_code)]
pub enum DateTimeParser {
    #[default]
    Default,
    Format(String),
    Custom(DateTimeParserFn),
}

impl DateTimeParser {
    pub fn parse(&self, datetime_str: &str) -> Result<i64, AquaTrollLogError> {
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

enum ArrayDataBuilder {
    DateTime(PrimitiveBuilder<TimestampSecondType>),
    Utf8(GenericStringBuilder<i32>),
    Float64(PrimitiveBuilder<Float64Type>),
}

pub(crate) struct TableBuilder {
    schema: Option<SchemaRef>,
    data_builders: Vec<ArrayDataBuilder>,
    datetime_parser: DateTimeParser,
}

impl TableBuilder {
    pub fn new() -> Self {
        Self {
            schema: None,
            data_builders: vec![],
            datetime_parser: DateTimeParser::Default,
        }
    }

    /// Specify field names and their data types.
    ///
    /// NOTE: The datatype will be assigned automatically based on field name
    pub fn field_names(mut self, field_names: Vec<String>) -> Self {
        let fields: Vec<Field> = field_names
            .into_iter()
            .map(|c| {
                if ["Date and Time", "Date Time", "Date/Time", "DateTime"].contains(&c.as_str()) {
                    // Rename datetime column and assign data type
                    Field::new(
                        "DateTime",
                        DataType::Timestamp(TimeUnit::Second, None),
                        false,
                    )
                } else if c == "Note" || c == "Marked" {
                    // Specific columns as string type
                    Field::new(c, DataType::Utf8, false)
                } else {
                    // Tread all other columns as metric values
                    Field::new(c, DataType::Float64, false)
                }
            })
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let mut data_builders = vec![];
        for field in schema.fields() {
            data_builders.push(match field.data_type() {
                DataType::Timestamp(TimeUnit::Second, None) => {
                    ArrayDataBuilder::DateTime(PrimitiveBuilder::<TimestampSecondType>::new())
                }
                DataType::Utf8 => ArrayDataBuilder::Utf8(GenericStringBuilder::<i32>::new()),
                DataType::Float64 => {
                    ArrayDataBuilder::Float64(PrimitiveBuilder::<Float64Type>::new())
                }
                _ => {
                    unreachable!()
                }
            })
        }

        self.schema = Some(schema);
        self.data_builders = data_builders;

        self
    }

    pub fn with_datetime_parser(mut self, parser: DateTimeParser) -> Self {
        self.datetime_parser = parser;
        self
    }

    pub fn try_push_row(mut self, row_values: Vec<String>) -> Result<Self, AquaTrollLogError> {
        let parser = &self.datetime_parser;
        for (value_str, builder) in row_values.into_iter().zip(&mut self.data_builders) {
            match builder {
                ArrayDataBuilder::DateTime(b) => {
                    let timestamp = parser.parse(&value_str)?;
                    b.append_value(timestamp)
                }
                ArrayDataBuilder::Utf8(b) => b.append_value(value_str),
                ArrayDataBuilder::Float64(b) => b.append_value(value_str.parse()?),
            }
        }

        Ok(self)
    }

    pub fn try_build(mut self) -> Result<RecordBatch, AquaTrollLogError> {
        let columns: Vec<_> = self
            .data_builders
            .iter_mut()
            .map(|builder| match builder {
                ArrayDataBuilder::DateTime(b) => Arc::new(b.finish()) as ArrayRef,
                ArrayDataBuilder::Utf8(b) => Arc::new(b.finish()) as ArrayRef,
                ArrayDataBuilder::Float64(b) => Arc::new(b.finish()) as ArrayRef,
            })
            .collect();

        Ok(RecordBatch::try_new(
            self.schema.ok_or(AquaTrollLogError::InvalidData)?,
            columns,
        )?)
    }
}

pub(crate) fn record_batch_to_json(batch: &RecordBatch) -> Result<Value, AquaTrollLogError> {
    let buf = Vec::new();
    let mut writer = ArrayWriter::new(buf);
    writer.write(batch)?;
    writer.finish()?;
    let json_data = writer.into_inner();

    Ok(serde_json::from_reader(json_data.as_slice())?)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Float64Array, StringArray};

    use super::*;

    #[test]
    fn datetime_str() {
        let datetime = "2021/7/20 PM 12:00:00";
        let timestamp = parse_datetime_str(datetime).unwrap();

        assert_eq!(timestamp, 1626753600);
    }

    #[test]
    fn parse_with_custom_format() {
        let result =
            parse_datetime_with_format("2021-07-20 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        assert_eq!(result, 1626753600);
    }

    #[test]
    fn parse_with_custom_format_alternative() {
        let result =
            parse_datetime_with_format("20/07/2021 12:00:00", "%d/%m/%Y %H:%M:%S").unwrap();
        assert_eq!(result, 1626753600);
    }

    #[test]
    fn table_builder_with_datetime_format() {
        let field_names = vec!["Date Time".to_string(), "Value".to_string()];
        let table_builder = TableBuilder::new()
            .field_names(field_names)
            .with_datetime_parser("%Y-%m-%d %H:%M:%S".into());

        let row_values = vec!["2021-07-20 12:00:00".to_string(), "1.0".to_string()];
        let table_builder = table_builder.try_push_row(row_values).unwrap();

        let record_batch = table_builder.try_build().unwrap();
        assert_eq!(record_batch.num_rows(), 1);
    }

    #[test]
    fn table_builder_with_custom_parser() {
        let parser = Rc::new(|s: &str| -> Result<i64, AquaTrollLogError> {
            // Parse format: DD/MM/YYYY HH:MM:SS
            parse_datetime_with_format(s, "%d/%m/%Y %H:%M:%S")
        }) as DateTimeParserFnRef;

        let field_names = vec!["Date Time".to_string(), "Value".to_string()];
        let table_builder = TableBuilder::new()
            .field_names(field_names)
            .with_datetime_parser(parser.into());

        let row_values = vec!["20/07/2021 12:00:00".to_string(), "1.0".to_string()];
        let table_builder = table_builder.try_push_row(row_values).unwrap();

        let record_batch = table_builder.try_build().unwrap();
        assert_eq!(record_batch.num_rows(), 1);
    }

    #[test]
    fn table_builder_default_parser_unchanged() {
        // Verify existing behavior still works
        let field_names = vec!["Date and Time".to_string(), "Value".to_string()];
        let table_builder = TableBuilder::new().field_names(field_names);

        let row_values = vec!["2021/7/20 PM 12:00:00".to_string(), "1.0".to_string()];
        let table_builder = table_builder.try_push_row(row_values).unwrap();

        let record_batch = table_builder.try_build().unwrap();
        assert_eq!(record_batch.num_rows(), 1);
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

        let record_batch = table_builder.try_build().unwrap();
        assert_eq!(record_batch.num_rows(), 2);
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
        let record_batch = table_builder.try_build().unwrap();

        assert_eq!(record_batch.num_columns(), 4);
        assert_eq!(record_batch.schema().field(0).name(), "DateTime");
        assert_eq!(record_batch.schema().field(1).name(), "Note");
        assert_eq!(record_batch.schema().field(3).name(), "Value");

        assert_eq!(record_batch.num_rows(), 2);
        assert_eq!(
            format!("{:?}", record_batch.column_by_name("Note").unwrap()),
            format!(
                "{:?}",
                StringArray::from(vec!["Foo".to_string(), "Bar".to_string()])
            )
        );
        assert_eq!(
            format!("{:?}", record_batch.column_by_name("Value").unwrap()),
            format!("{:?}", Float64Array::from(vec![1.0, 2.0]))
        )
    }
}
