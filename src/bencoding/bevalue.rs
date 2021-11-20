use std::fmt;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;
use std::{collections::HashMap, ops::Range};

use thiserror::Error;

use super::{BeDecodeError, BeParser};

#[derive(PartialEq)]
pub enum BeValue {
    Str(Str),
    Int(Int),
    Dict(Dict),
    List(List),
}

pub type Str = Vec<u8>;
pub type Int = i64;
pub type List = Vec<BeValue>;

#[derive(PartialEq, Default)]
pub struct Dict {
    vals: HashMap<String, BeValue>,
    /// For getting the binary data from the source. Used for computing the hash.
    pub src_range: Range<usize>,
}

impl Dict {
    pub fn new(vals: HashMap<String, BeValue>, src_range: Range<usize>) -> Self {
        Dict { vals, src_range }
    }

    pub fn expect(&mut self, k: &str) -> Result<&mut BeValue, ResponseParseError> {
        self.vals
            .get_mut(k)
            .ok_or_else(|| ResponseParseError::ValNotContained(k.to_string()))
    }
}

impl BeValue {
    pub fn from_bytes(src: &[u8]) -> Result<Self, BeDecodeError> {
        BeParser::parse_with(src)
    }

    pub fn take_dict(&mut self) -> ReponseParseResult<Dict> {
        match self {
            BeValue::Dict(d) => Ok(std::mem::take(d)),
            t => Err(ResponseParseError::InvalidType("dictionary", t.label())),
        }
    }

    pub fn take_str(&mut self) -> ReponseParseResult<Str> {
        match self {
            BeValue::Str(s) => Ok(std::mem::take(s)),
            t => Err(ResponseParseError::InvalidType("string", t.label())),
        }
    }

    pub fn take_str_utf8(&mut self) -> ReponseParseResult<String> {
        match self {
            BeValue::Str(s) => Ok(String::from_utf8(s.to_vec())?),
            t => Err(ResponseParseError::InvalidType("string", t.label())),
        }
    }

    pub fn take_int(&mut self) -> ReponseParseResult<Int> {
        match self {
            BeValue::Int(i) => Ok(*i),
            t => Err(ResponseParseError::InvalidType("integer", t.label())),
        }
    }

    pub fn take_uint(&mut self) -> ReponseParseResult<u64> {
        match self {
            BeValue::Int(i) => Ok(u64::try_from(*i)?),
            t => Err(ResponseParseError::InvalidType("integer", t.label())),
        }
    }

    const INDENT_LEN: usize = 4;

    pub fn inner_fmt(&self, f: &mut fmt::Formatter<'_>, depth: usize) -> fmt::Result {
        match self {
            BeValue::Str(s) => {
                let lossy_string = String::from_utf8_lossy(s);
                f.write_fmt(format_args!("{}\n", lossy_string))
            }
            BeValue::Int(i) => f.write_fmt(format_args!("{}\n", i)),
            BeValue::Dict(d) => {
                f.write_fmt(format_args!("Dict\n"))?;
                for (key, val) in d.vals.iter() {
                    f.write_str(&" ".repeat((depth - 1) * Self::INDENT_LEN))?;
                    f.write_str("|")?;
                    f.write_str(&" ".repeat(Self::INDENT_LEN - 1))?;

                    f.write_fmt(format_args!("{}: ", key))?;
                    if key == "pieces" {
                        f.write_str("<piece hashes...>\n")?;
                    } else {
                        val.inner_fmt(f, depth + 1)?;
                    }
                }

                Ok(())
            }
            BeValue::List(l) => {
                f.write_fmt(format_args!("List\n"))?;
                for val in l.iter() {
                    f.write_str(&" ".repeat((depth - 1) * Self::INDENT_LEN))?;
                    f.write_str("|")?;
                    f.write_str(&" ".repeat(Self::INDENT_LEN - 1))?;
                    val.inner_fmt(f, depth + 1)?;
                }

                Ok(())
            }
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
    #[error("Attempted to take a value of type '{0}' when 'self' is '{1}'")]
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
        self.inner_fmt(f, 1)
    }
}
