mod common;
mod csv_reader;
mod html_reader;
mod txt_reader;

pub(crate) use csv_reader::read_table as read_csv_table;
pub(crate) use html_reader::{read_html, read_zipped_html};
pub(crate) use txt_reader::{read_attr, read_log_data_attr, read_table};
