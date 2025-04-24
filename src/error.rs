#[derive(thiserror::Error, Debug)]
pub enum InSituLogError {
    #[error(transparent)]
    StdIoError(#[from] std::io::Error),
    #[error(transparent)]
    ChronoParseError(#[from] chrono::ParseError),
    #[error(transparent)]
    ParseFloatError(#[from] std::num::ParseFloatError),
    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error(transparent)]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    ArrowSchemaError(#[from] arrow_schema::ArrowError),
    #[error(transparent)]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
    #[error(transparent)]
    CsvError(#[from] csv::Error),
    #[error(transparent)]
    ZipError(#[from] zip::result::ZipError),
    #[error("Unexpected EOF")]
    UnexpectedEof,
    #[error("html file: section header not found")]
    SectionHeaderNotFound,
    #[error("Invalid Data")]
    InvalidData,
}
