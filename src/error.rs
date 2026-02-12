#[derive(thiserror::Error, Debug)]
pub struct ErrorWithPartialResult {
    pub result: Box<crate::AquaTrollLogData>,
    pub errors: Vec<csv::Error>,
}

impl std::fmt::Display for ErrorWithPartialResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Data log error with partial result:")?;
        for e in &self.errors {
            writeln!(f, "{e}")?;
        }

        Ok(())
    }
}

#[allow(clippy::result_large_err)]
#[derive(thiserror::Error, Debug)]
pub enum AquaTrollLogError {
    #[error(transparent)]
    StdIoError(#[from] std::io::Error),
    #[error(transparent)]
    ChronoParseError(#[from] chrono::ParseError),
    #[error(transparent)]
    ParseFloatError(#[from] std::num::ParseFloatError),
    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error(transparent)]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
    #[error(transparent)]
    CsvError(#[from] csv::Error),
    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),
    #[error(transparent)]
    ZipError(#[from] zip::result::ZipError),
    #[error("Unexpected EOF")]
    UnexpectedEof,
    #[error("html file: section header not found")]
    SectionHeaderNotFound,
    #[error("Invalid Data")]
    InvalidData,
    #[error(transparent)]
    WithCsvPartialResult(#[from] crate::util::csv_reader::ErrorWithCsvPartialResult),
    #[error(transparent)]
    WithPartialResult(#[from] ErrorWithPartialResult),
}
