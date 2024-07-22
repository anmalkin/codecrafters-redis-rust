use std::fmt::{Debug, Display};

#[derive(Debug)]
pub enum RESPError {
    UnexpectedEnd,
    UnknownStartingByte,
    IOError(std::io::Error),
    IntParseFailure,
    BadBulkStringSize(i64),
    BadArraySize(i64),
    InvalidCommand,
}

impl From<std::io::Error> for RESPError {
    fn from(value: std::io::Error) -> Self {
        RESPError::IOError(value)
    }
}

impl Display for RESPError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RESPError::UnexpectedEnd => write!(f, "Unexpected end of input."),
            RESPError::UnknownStartingByte => write!(f, "Unknown starting byte."),
            RESPError::IOError(e) => write!(f, "{}", e),
            RESPError::IntParseFailure => write!(f, "Failed to parse int."),
            RESPError::BadBulkStringSize(size) => {
                write!(f, "Invalid bulk string size of {} bytes.", size)
            }
            RESPError::BadArraySize(size) => write!(f, "Invalid array size of {} bytes.", size),
            RESPError::InvalidCommand => write!(f, "Invalid command."),
        }
    }
}
