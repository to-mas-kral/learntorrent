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

// TODO: should check for file size / recursion depth, so the parser doesn't crash or allocate too much memory

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

    /** i<integer encoded in base ten ASCII>e **/
    fn parse_int(&mut self) -> Result<i64, BeError> {
        self.next().unwrap(); // Skip the "i"

        // TODO: don't allocate here
        let mut digits = String::with_capacity(12);

        loop {
            match self.next() {
                Some(b) => match b {
                    b if b.is_ascii_digit() || *b == b'-' => digits.push(*b as char),
                    b'e' => break,
                    _ => {
                        return Err(BeError::InvalidInt(
                            digits,
                            "next byte wasn't a valid integer character",
                        ));
                    }
                },
                None => return Err(BeError::UnexpectedEnd),
            }
        }

        if digits.is_empty() {
            return Err(BeError::InvalidInt(digits, "integer is empty"));
        }

        // Manual number conversion would be better
        digits
            .parse::<i64>()
            .map_err(|_| BeError::InvalidInt(digits, "not a proper integer"))
    }

    fn parse_str(&mut self) -> Result<Vec<u8>, BeError> {
        // TODO: don't allocate here
        let mut digits = String::with_capacity(4);

        loop {
            match self.peek() {
                Some(b) if b.is_ascii_digit() => digits.push(*self.next().unwrap() as char),
                Some(_) => break,
                None => return Err(BeError::UnexpectedEnd),
            }
        }

        self.expect(b':', "Expected a colon while parsing a string")?;

        let len = digits
            .parse::<u64>()
            .map_err(|_| BeError::InvalidStrLen(digits, "not a proper string length"))?;

        let mut string = Vec::new();
        for _ in 0..len {
            match self.next() {
                Some(b) => string.push(*b),
                None => return Err(BeError::UnexpectedEnd),
            }
        }

        Ok(string)
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

    /** d<bencoded string><bencoded element>e
     * dictionary keys must be valid UTF-8
     * **/
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
    InvalidInt(String, &'static str),
    #[error("Found an invalid string length: {0}, cause: '{0}'")]
    InvalidStrLen(String, &'static str),
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
