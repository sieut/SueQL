use bincode;

#[derive(Debug)]
pub enum Error {
    IoError(std::io::Error),
    SerdeError(bincode::Error),
    // TODO Update errors with more debug info
    CorruptedData,
    Internal(String),
}

impl Error {
    pub fn io_kind(&self) -> Option<std::io::ErrorKind> {
        match &self {
            &Error::IoError(err) => Some(err.kind()),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::IoError(e)
    }
}

impl From<bincode::Error> for Error {
    fn from(e: bincode::Error) -> Self {
        Error::SerdeError(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
