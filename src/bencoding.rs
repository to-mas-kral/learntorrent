use std::collections::HashMap;

use thiserror::Error;

use self::bevalue::{BeValue, Dict};

pub mod bevalue;

// TODO: should check for recursion depth

pub struct BeParser<'s> {
    src: &'s [u8],
    pos: usize,
}

impl<'s> BeParser<'s> {
    pub fn new(src: &'s [u8]) -> Self {
        Self { src, pos: 0 }
    }

    pub fn parse_with(src: &'s [u8]) -> Result<BeValue, BeDecodeErr> {
        let mut parser = Self::new(src);
        parser.parse_value()
    }

    pub fn parse_value(&mut self) -> Result<BeValue, BeDecodeErr> {
        match self.peek() {
            Some(b'i') => Ok(BeValue::Int(self.parse_int()?)),
            Some(b'l') => Ok(BeValue::List(self.parse_list()?)),
            Some(b'd') => Ok(BeValue::Dict(self.parse_dict()?)),
            Some(c) if c.is_ascii_digit() => Ok(BeValue::Str(self.parse_str()?)),
            Some(i) => Err(BeDecodeErr::InvalidInitialByte(*i)),
            None => Err(BeDecodeErr::UnexpectedEnd),
        }
    }

    /// i<integer encoded in base ten ASCII>e
    fn parse_int(&mut self) -> Result<i64, BeDecodeErr> {
        self.next().unwrap(); // Skip the "i"

        let minus_one = if self.peek() == Some(&b'-') {
            self.next();
            -1
        } else {
            1
        };

        let num = i64::try_from(self.parse_digits()?)?;

        self.expect(b'e')?;

        Ok(minus_one * num)
    }

    fn parse_str(&mut self) -> Result<Vec<u8>, BeDecodeErr> {
        let len = self.parse_digits()?;

        self.expect(b':')?;

        // TODO: have some allocation limits
        let mut string = Vec::with_capacity(len as usize);
        for _ in 0..len {
            match self.next() {
                Some(b) => string.push(*b),
                None => return Err(BeDecodeErr::UnexpectedEnd),
            }
        }

        Ok(string)
    }

    fn parse_digits(&mut self) -> Result<u64, BeDecodeErr> {
        let digits = self.take_while(|b| b.is_ascii_digit())?;
        // UNWRAP: safe becasue we are only accepting ASCII digits
        let digits = std::str::from_utf8(digits).unwrap();

        let num = digits
            .parse::<u64>()
            .map_err(|_| BeDecodeErr::InvalidStrLen(digits.to_string()))?;

        Ok(num)
    }

    fn parse_list(&mut self) -> Result<Vec<BeValue>, BeDecodeErr> {
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
                None => return Err(BeDecodeErr::UnexpectedEnd),
            }
        }

        Ok(list)
    }

    /// d<bencoded string><bencoded element>e
    fn parse_dict(&mut self) -> Result<Dict, BeDecodeErr> {
        let start = self.pos;
        self.next().unwrap(); // Skip the "d"

        let mut dict = HashMap::new();

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
                None => return Err(BeDecodeErr::UnexpectedEnd),
            }
        }

        let end = self.pos;
        Ok(Dict::new(dict, start..end))
    }

    fn take_while(&mut self, condition: fn(u8) -> bool) -> Result<&[u8], BeDecodeErr> {
        let start = self.pos;

        loop {
            match self.peek() {
                Some(b) if condition(*b) => self.pos += 1,
                Some(_) => return Ok(&self.src[start..self.pos]),
                None => return Err(BeDecodeErr::UnexpectedEnd),
            }
        }
    }

    fn expect(&mut self, expected: u8) -> Result<(), BeDecodeErr> {
        match self.next() {
            Some(c) if *c == expected => Ok(()),
            o => Err(BeDecodeErr::Unexpected(o.cloned())),
        }
    }

    fn next(&mut self) -> Option<&u8> {
        let ret = self.src.get(self.pos);
        self.pos += 1;
        ret
    }

    fn peek(&self) -> Option<&u8> {
        self.src.get(self.pos)
    }
}

#[derive(Error, Debug)]
pub enum BeDecodeErr {
    #[error("The byte stream ended unexpectedly")]
    UnexpectedEnd,
    #[error("Initial element character should be either a digit, 'i', 'l' or 'd', got: {0}")]
    InvalidInitialByte(u8),
    #[error("The integer: {0}, is not a valid string length")]
    InvalidStrLen(String),
    #[error("Invalid number: '{0}")]
    InvalidNum(#[from] std::num::TryFromIntError),
    #[error("Unexpected byte: {0:?}")]
    Unexpected(Option<u8>),
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
}
