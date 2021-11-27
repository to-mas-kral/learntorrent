use bytes::{Buf, BytesMut};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Framed};

/// All of the remaining messages in the protocol take the form of \<length prefix>\<message ID>\<payload>.
/// The length prefix is a four byte big-endian value. The message ID is a single decimal byte.
/// The payload is message dependent.
#[derive(Debug, PartialEq)]
pub enum Message {
    /// keep-alive: <len=0000>
    KeepAlive,
    /// choke: <len=0001><id=0>
    Choke,
    /// unchoke: <len=0001><id=1>
    Unchoke,
    /// interested: <len=0001><id=2>
    Interested,
    /// not interested: <len=0001><id=3>
    NotInterested,
    /// have: <len=0005><id=4>\<piece index>
    Have(u32),
    /// bitfield: <len=0001+X><id=5>\<bitfield>
    Bitfield(BytesMut),
    /// request: <len=0013><id=6>\<index>\<begin>\<length>
    Request { index: u32, begin: u32, len: u32 },
    /// piece: <len=0009+X><id=7>\<index>\<begin>\<block>
    Piece {
        index: u32,
        begin: u32,
        block: BytesMut,
    },
    /// cancel: <len=0013><id=8>\<index>\<begin>\<length>
    Cancel { index: u32, begin: u32, len: u32 },
}

pub struct MessageCodec;

#[derive(Error, Debug)]
pub enum MessageDecoderErr {
    #[error("IO error: '{0}'")]
    Io(#[from] std::io::Error),
    #[error("Invalid message with length: '{0}', id: '{1}'")]
    InvalidMessage(u32, u8),
    #[error("The 'port' message is unimplemented")]
    PortUnimplemented,
}

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = MessageDecoderErr;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> Result<Option<<Self as Decoder>::Item>, <Self as Decoder>::Error> {
        const LEN_MARKER_SIZE: usize = 4;
        const ID_MARKER_SIZE: usize = 1;

        // Not enough bytes for the length
        if src.len() < LEN_MARKER_SIZE {
            return Ok(None);
        }

        // Read length marker.
        // ! We can't advance here because we don't know if we received the whole message !
        let mut len = [0u8; LEN_MARKER_SIZE];
        len.copy_from_slice(&src[..LEN_MARKER_SIZE]);
        let len = u32::from_be_bytes(len) as usize;

        if len == 0 {
            src.advance(LEN_MARKER_SIZE);
            return Ok(Some(Message::KeepAlive));
        }

        // Not enough bytes for the ID
        if src.len() < LEN_MARKER_SIZE + ID_MARKER_SIZE {
            return Ok(None);
        }

        let id = src[4];

        match (len, id) {
            (1, id) if id <= 3 => {
                src.advance(len + LEN_MARKER_SIZE);
                match id {
                    0 => Ok(Some(Message::Choke)),
                    1 => Ok(Some(Message::Unchoke)),
                    2 => Ok(Some(Message::Interested)),
                    3 => Ok(Some(Message::NotInterested)),
                    _ => Err(MessageDecoderErr::InvalidMessage(1, id)),
                }
            }
            _ => {
                if src.len() < (len + LEN_MARKER_SIZE) {
                    return Ok(None);
                }
                src.advance(5);

                match (len, id) {
                    // Have
                    (5, 4) => {
                        let piece_index = src.get_u32();
                        Ok(Some(Message::Have(piece_index)))
                    }
                    // Bitfield
                    (len, 5) if len > 1 => {
                        let bitfield = src.split_to(len - 1);
                        Ok(Some(Message::Bitfield(bitfield)))
                    }
                    // Request
                    (13, 6) => {
                        let index = src.get_u32();
                        let begin = src.get_u32();
                        let len = src.get_u32();
                        Ok(Some(Message::Request { index, begin, len }))
                    }
                    // Piece
                    (len, 7) => {
                        let index = src.get_u32();
                        let begin = src.get_u32();
                        let block = src.split_to(len - 9);
                        Ok(Some(Message::Piece {
                            index,
                            begin,
                            block,
                        }))
                    }
                    // Cancel
                    (13, 8) => {
                        let index = src.get_u32();
                        let begin = src.get_u32();
                        let len = src.get_u32();
                        Ok(Some(Message::Cancel { index, begin, len }))
                    }
                    // Port
                    (3, 9) => Err(MessageDecoderErr::PortUnimplemented),
                    (len, id) => Err(MessageDecoderErr::InvalidMessage(len as u32, id)),
                }
            }
        }
    }
}

#[cfg(test)]
mod test_message {
    use bytes::BufMut;

    use super::*;

    #[test]
    fn test_decode() {
        let mut decoder = MessageCodec;

        let mut bytes = BytesMut::new();
        // TODO: replace with Encoder once thta's done
        #[rustfmt::skip]
        bytes.put_slice(&[
            //len          id    payload
            0, 0, 0, 0,
            0, 0, 0, 1,    0,
            0, 0, 0, 1,    2,
            0, 0, 0, 5,    4,    0, 0, 48, 57,
        ]);

        let res = decoder.decode(&mut bytes).unwrap();
        assert_eq!(res, Some(Message::KeepAlive));
        let res = decoder.decode(&mut bytes).unwrap();
        assert_eq!(res, Some(Message::Choke));
        let res = decoder.decode(&mut bytes).unwrap();
        assert_eq!(res, Some(Message::Interested));
        let res = decoder.decode(&mut bytes).unwrap();
        assert_eq!(res, Some(Message::Have(12345)));
    }

    #[test]
    fn test_decode_bitfield() {
        let mut decoder = MessageCodec;

        let mut bytes = BytesMut::new();
        bytes.put_slice(&[0, 0, 0, 10, 5, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let res = decoder.decode(&mut bytes).unwrap();

        let expected = vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9];
        let expected = BytesMut::from(&expected[..]);
        assert_eq!(res, Some(Message::Bitfield(expected)));
    }

    #[test]
    fn test_decode_invalid_bitfield() {
        let mut decoder = MessageCodec;

        let mut bytes = BytesMut::new();
        bytes.put_slice(&[0, 0, 0, 1, 5]);
        decoder.decode(&mut bytes).unwrap_err();
    }

    #[test]
    fn test_decode_piece() {
        let mut decoder = MessageCodec;

        let mut bytes = BytesMut::new();
        #[rustfmt::skip]
        bytes.put_slice(&[
            0, 0, 0, 15,    7,
            0, 4, 0, 0,
            2, 0, 0, 0,
            6, 5, 4, 3, 2, 1
        ]);
        let res = decoder.decode(&mut bytes).unwrap();

        let block = vec![6u8, 5, 4, 3, 2, 1];
        let block = BytesMut::from(&block[..]);
        assert_eq!(
            res,
            Some(Message::Piece {
                index: 262144,
                begin: 33554432,
                block
            })
        );
    }
}
