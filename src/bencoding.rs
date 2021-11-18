use std::{collections::HashMap, fmt};
use thiserror::Error;

#[derive(PartialEq)]
pub enum BeValue {
    Str(Vec<u8>),
    Int(i64),
    Dict(Dict),
    List(Vec<BeValue>),
}

type Dict = HashMap<String, BeValue>;

impl BeValue {
    pub fn from_bytes(src: &[u8]) -> Result<Self, BeError> {
        BeParser::parse_with(src)
    }

    const INDENT_LEN: usize = 2;

    pub fn inner_fmt(&self, f: &mut fmt::Formatter<'_>, depth: usize) -> fmt::Result {
        match self {
            BeValue::Str(s) => {
                let lossy_string = String::from_utf8_lossy(s);
                f.write_fmt(format_args!("{}\n", lossy_string))
            }
            BeValue::Int(i) => f.write_fmt(format_args!("{}\n", i)),
            BeValue::Dict(d) => {
                f.write_fmt(format_args!("Dict\n"))?;
                for (key, val) in d.iter() {
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
}

impl fmt::Debug for BeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner_fmt(f, 1)
    }
}

// TODO: should check for recursion depth

pub struct BeParser<'s> {
    src: &'s [u8],
}

impl<'s> BeParser<'s> {
    pub fn new(src: &'s [u8]) -> Self {
        Self { src }
    }

    pub fn parse_with(src: &'s [u8]) -> Result<BeValue, BeError> {
        let mut parser = Self::new(src);
        parser.parse_value()
    }

    pub fn parse_value(&mut self) -> Result<BeValue, BeError> {
        match self.peek() {
            Some(b'i') => Ok(BeValue::Int(self.parse_int()?)),
            Some(b'l') => Ok(BeValue::List(self.parse_list()?)),
            Some(b'd') => Ok(BeValue::Dict(self.parse_dict()?)),
            Some(c) if c.is_ascii_digit() => Ok(BeValue::Str(self.parse_str()?)),
            Some(i) => Err(BeError::InvalidInitialByte(*i)),
            None => Err(BeError::UnexpectedEnd),
        }
    }

    /// i<integer encoded in base ten ASCII>e
    fn parse_int(&mut self) -> Result<i64, BeError> {
        self.next().unwrap(); // Skip the "i"

        let minus_one = if self.peek() == Some(&b'-') {
            self.next();
            -1
        } else {
            1
        };

        let num = i64::try_from(self.parse_digits()?)?;

        self.expect(b'e', "Expected end of integer")?;

        Ok(minus_one * num)
    }

    fn parse_str(&mut self) -> Result<Vec<u8>, BeError> {
        let len = self.parse_digits()?;

        self.expect(b':', "Expected a colon while parsing a string")?;

        // TODO: have some allocation limits
        let mut string = Vec::with_capacity(len as usize);
        for _ in 0..len {
            match self.next() {
                Some(b) => string.push(*b),
                None => return Err(BeError::UnexpectedEnd),
            }
        }

        Ok(string)
    }

    fn parse_digits(&mut self) -> Result<u64, BeError> {
        let digits = self.take_while(|b| b.is_ascii_digit())?;
        // This shouldn't panic since we are only accepting ASCII digits...
        let digits = std::str::from_utf8(digits).unwrap();

        let num = digits.parse::<u64>().map_err(|_| {
            BeError::InvalidStrLen(digits.to_string(), "not a proper string length")
        })?;

        Ok(num)
    }

    fn parse_list(&mut self) -> Result<Vec<BeValue>, BeError> {
        self.next().unwrap(); // Skip the "l"

        let mut list = Vec::new();

        loop {
            match self.peek() {
                Some(b) => match b {
                    b'e' => {
                        self.next().unwrap();
                        break;
                    }
                    _ => list.push(self.parse_value()?),
                },
                None => return Err(BeError::UnexpectedEnd),
            }
        }

        Ok(list)
    }

    /// d<bencoded string><bencoded element>e
    fn parse_dict(&mut self) -> Result<Dict, BeError> {
        self.next().unwrap(); // Skip the "d"

        let mut dict = Dict::new();

        loop {
            match self.peek() {
                Some(b) => match b {
                    b'e' => {
                        self.next().unwrap();
                        break;
                    }
                    _ => {
                        // TODO: verify if dict keys have to be UTF-8 or just bytes
                        let key = String::from_utf8(self.parse_str()?)?;
                        let val = self.parse_value()?;

                        // TODO: should probably check for non-unique keys (and sorted ?)
                        dict.insert(key, val);
                    }
                },
                None => return Err(BeError::UnexpectedEnd),
            }
        }

        Ok(dict)
    }

    fn take_while(&mut self, condition: fn(u8) -> bool) -> Result<&[u8], BeError> {
        let mut count = 0;

        loop {
            match self.src.get(count) {
                Some(b) if condition(*b) => count += 1,
                Some(_) => {
                    let (head, tail) = self.src.split_at(count);
                    self.src = tail;
                    return Ok(head);
                }
                None => return Err(BeError::UnexpectedEnd),
            }
        }
    }

    fn expect(&mut self, expected: u8, error_msg: &'static str) -> Result<(), BeError> {
        match self.next() {
            Some(c) if *c == expected => Ok(()),
            o => Err(BeError::Expected(error_msg, o.cloned())),
        }
    }

    fn next(&mut self) -> Option<&u8> {
        match self.src.split_first() {
            None => None,
            Some((head, tail)) => {
                self.src = tail;
                Some(head)
            }
        }
    }

    fn peek(&self) -> Option<&u8> {
        self.src.get(0)
    }
}

#[derive(Error, Debug)]
pub enum BeError {
    #[error("The byte stream ended unexpectedly")]
    UnexpectedEnd,
    #[error("Initial element character should be either a digit, 'i', 'l' or 'd', got: {0}")]
    InvalidInitialByte(u8),
    #[error("Found an invalid integer: {0}, cause: '{0}'")]
    InvalidStrLen(String, &'static str),
    #[error("Invalid number")]
    InvalidNum(#[from] std::num::TryFromIntError),
    #[error("{0}, got: {1:?}")]
    Expected(&'static str, Option<u8>),
    #[error("Dictionary key must be a valid UTF-8 string. {0}")]
    InvalidDictKey(#[from] std::string::FromUtf8Error),
}

#[cfg(test)]
mod test {
    use super::{BeParser, BeValue};

    #[test]
    fn test_correct_integer_decoding() {
        assert_eq!(
            BeParser::parse_with("i12345e".as_bytes()).unwrap(),
            BeValue::Int(12345)
        );

        assert_eq!(
            BeParser::parse_with("i0e".as_bytes()).unwrap(),
            BeValue::Int(0)
        );

        assert_eq!(
            BeParser::parse_with("i-1e".as_bytes()).unwrap(),
            BeValue::Int(-1)
        );
    }

    #[test]
    fn test_incorrect_integer_decoding() {
        BeParser::parse_with("ie".as_bytes()).expect_err("");
        BeParser::parse_with("ia1e".as_bytes()).expect_err("");
        BeParser::parse_with("i1ae".as_bytes()).expect_err("");
        BeParser::parse_with("ia1be".as_bytes()).expect_err("");
        // TODO: more number parsing validation (i-0e is invalid, leading zeros other than i0e are invalid)
        //BeParser::parse_with("i-0e".as_bytes()).expect_err("");
    }

    #[test]
    fn test_correct_string_decoding() {
        assert_eq!(
            BeParser::parse_with("1:a".as_bytes()).unwrap(),
            BeValue::Str(vec![b'a'])
        );
        assert_eq!(
            BeParser::parse_with("0:".as_bytes()).unwrap(),
            BeValue::Str(vec![])
        );
        assert_eq!(
            BeParser::parse_with("3:i1e".as_bytes()).unwrap(),
            BeValue::Str(vec![b'i', b'1', b'e'])
        );
    }

    #[test]
    fn test_incorrect_string_decoding() {
        BeParser::parse_with("-1:sds".as_bytes()).expect_err("");
        BeParser::parse_with("9b9:ia1e".as_bytes()).expect_err("");
        BeParser::parse_with("5.5:10".as_bytes()).expect_err("");
        BeParser::parse_with("2ae".as_bytes()).expect_err("");
    }

    // TODO: more tests
}
