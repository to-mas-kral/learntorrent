use std::{collections::HashMap, fmt, num::TryFromIntError, ops::Range, string::FromUtf8Error};

use thiserror::Error;

use super::{BeDecodeErr, BeParser};

#[derive(PartialEq)]
pub enum BeValue {
    Str(BeStr),
    Int(BeInt),
    Dict(Dict),
    List(BeList),
}

// String needlessly allocates, but this is not performance-critical code
pub type BeStr = Vec<u8>;
pub type BeInt = i64;
pub type BeList = Vec<BeValue>;

#[derive(PartialEq, Default)]
pub struct Dict {
    vals: HashMap<String, BeValue>,
    /// Used for computing the hash of a specific dict.
    pub src_range: Range<usize>,
}

impl Dict {
    pub fn new(vals: HashMap<String, BeValue>, src_range: Range<usize>) -> Self {
        Dict { vals, src_range }
    }

    /// Return a reference to a compulsory field
    pub fn expect(&mut self, k: &str) -> ReponseParseResult<&mut BeValue> {
        self.vals
            .get_mut(k)
            .ok_or_else(|| ResponseParseError::ValNotContained(k.to_string()))
    }

    /// Get a mutable reference to an optional field
    pub fn get_mut(&mut self, k: &str) -> Option<&mut BeValue> {
        self.vals.get_mut(k)
    }

    /// Extracts an optional field into a specific type
    pub fn try_get<T, F>(&mut self, k: &str, f: F) -> ReponseParseResult<Option<T>>
    where
        F: FnOnce(&mut BeValue) -> ReponseParseResult<T>,
    {
        let res = self.get_mut(k).map(f);

        res.transpose()
    }

    // TODO(commit): solve lifetime issue by using |BeVaule| -> &mut T in the previous method
    /// Extracts an optional field into a specific type
    pub fn try_get_ref_mut<T, F>(&mut self, k: &str, f: F) -> ReponseParseResult<Option<&mut T>>
    where
        F: FnOnce(&mut BeValue) -> ReponseParseResult<&mut T>,
    {
        let res = self.get_mut(k).map(f);

        res.transpose()
    }
}

impl BeValue {
    pub fn from_bytes(src: &[u8]) -> Result<Self, BeDecodeErr> {
        BeParser::parse_with(src)
    }

    pub fn get_dict(&mut self) -> ReponseParseResult<&mut Dict> {
        match self {
            BeValue::Dict(d) => Ok(d),
            t => Err(ResponseParseError::InvalidType("dictionary", t.label())),
        }
    }

    pub fn get_list(&mut self) -> ReponseParseResult<&mut BeList> {
        match self {
            BeValue::List(l) => Ok(l),
            t => Err(ResponseParseError::InvalidType("dictionary", t.label())),
        }
    }

    pub fn get_str(&mut self) -> ReponseParseResult<BeStr> {
        match self {
            BeValue::Str(s) => Ok(s.clone()),
            t => Err(ResponseParseError::InvalidType("string", t.label())),
        }
    }

    pub fn get_str_utf8(&mut self) -> ReponseParseResult<String> {
        match self {
            BeValue::Str(s) => Ok(String::from_utf8(s.to_vec())?),
            t => Err(ResponseParseError::InvalidType("string", t.label())),
        }
    }

    pub fn get_u64(&mut self) -> ReponseParseResult<u64> {
        match self {
            BeValue::Int(i) => Ok(u64::try_from(*i)?),
            t => Err(ResponseParseError::InvalidType("integer", t.label())),
        }
    }

    pub fn get_u32(&mut self) -> ReponseParseResult<u32> {
        match self {
            BeValue::Int(i) => Ok(u32::try_from(*i)?),
            t => Err(ResponseParseError::InvalidType("integer", t.label())),
        }
    }

    fn label(&self) -> &'static str {
        match self {
            BeValue::Str(_) => "string",
            BeValue::Int(_) => "integer",
            BeValue::Dict(_) => "dictionary",
            BeValue::List(_) => "list",
        }
    }
}

pub type ReponseParseResult<T> = Result<T, ResponseParseError>;

#[derive(Error, Debug)]
pub enum ResponseParseError {
    #[error("Attempted to extract a value of type '{0}' when 'self' is '{1}'")]
    InvalidType(&'static str, &'static str),
    #[error("The value of key: '{0}' is not contained in this dictionary")]
    ValNotContained(String),
    #[error("Error while getting an unsigned int: '{0}'")]
    IntNotUnsigned(#[from] TryFromIntError),
    #[error("String isn't properly UTF-8 formatted: '{0}'")]
    StrNotUtf8(#[from] FromUtf8Error),
}

impl fmt::Debug for BeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BeValue::Str(s) => {
                let lossy_string = String::from_utf8_lossy(s);
                f.write_fmt(format_args!("{}", lossy_string))
            }
            BeValue::Int(i) => f.write_fmt(format_args!("{}", i)),
            BeValue::Dict(d) => f
                .debug_map()
                .entries(
                    d.vals
                        .iter()
                        .filter(|(k, _)| *k != "pieces" && *k != "piece_hashes"),
                )
                .finish(),
            BeValue::List(l) => f.debug_list().entries(l.iter()).finish(),
        }
    }
}
