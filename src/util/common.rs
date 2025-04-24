use std::sync::Arc;

use arrow::{
    array::{ArrayRef, GenericStringBuilder, PrimitiveBuilder, RecordBatch},
    datatypes::{DataType, Field, Float64Type, Schema, SchemaRef, TimeUnit, TimestampSecondType},
};
use chrono::{FixedOffset, NaiveDateTime};

use crate::error::InSituLogError;

pub(crate) fn parse_datetime_str(datetime: &str) -> Result<i64, InSituLogError> {
    let tz = FixedOffset::east_opt(8 * 3600).unwrap();
    Ok(
        NaiveDateTime::parse_from_str(datetime, "%Y/%-m/%-d %p %I:%M:%S")
            .or_else(|_| NaiveDateTime::parse_from_str(datetime, "%Y/%-m/%-d %I:%M:%S %p"))
            .or_else(|_| NaiveDateTime::parse_from_str(datetime, "%Y-%-m-%-d %H:%M:%S"))
            .map(|t| t.and_local_timezone(tz).unwrap())
            .map(|t| t.to_utc())
            .map(|t| t.timestamp())?,
    )
}

enum ArrayDataBuilder {
    DateTime(PrimitiveBuilder<TimestampSecondType>),
    Utf8(GenericStringBuilder<i32>),
    Float64(PrimitiveBuilder<Float64Type>),
}

pub(crate) struct TableBuilder {
    schema: Option<SchemaRef>,
    data_builders: Vec<ArrayDataBuilder>,
}

impl TableBuilder {
    pub fn new() -> Self {
        Self {
            schema: None,
            data_builders: vec![],
        }
    }

    pub fn field_names(mut self, field_names: Vec<String>) -> Self {
        let fields: Vec<Field> = field_names
            .into_iter()
            .map(|c| {
                if ["Date and Time", "Date Time", "Date/Time"].contains(&c.as_str()) {
                    Field::new(c, DataType::Timestamp(TimeUnit::Second, None), false)
                } else if c == "Note" || c == "Marked" {
                    Field::new(c, DataType::Utf8, false)
                } else {
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

    pub fn try_push_row(mut self, row_values: Vec<String>) -> Result<Self, InSituLogError> {
        for (value_str, builder) in row_values.into_iter().zip(&mut self.data_builders) {
            match builder {
                ArrayDataBuilder::DateTime(b) => b.append_value(parse_datetime_str(&value_str)?),
                ArrayDataBuilder::Utf8(b) => b.append_value(value_str),
                ArrayDataBuilder::Float64(b) => b.append_value(value_str.parse()?),
            }
        }

        Ok(self)
    }

    pub fn try_build(mut self) -> Result<RecordBatch, InSituLogError> {
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
            self.schema.ok_or(InSituLogError::InvalidData)?,
            columns,
        )?)
    }
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
        assert_eq!(record_batch.schema().field(0).name(), "Date and Time");
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
