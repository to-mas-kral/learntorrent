use sha1::{Digest, Sha1};
use thiserror::Error;

use crate::bencoding::bevalue::{BeValue, ResponseParseError};

#[derive(Debug)]
pub struct MetaInfo {
    pub announce: String,
    pub info_hash: Vec<u8>,
    pub piece_length: u64,
    pub total_length: u64,
    pub name: String,
    pub piece_hashes: PieceHashes,
    // TODO: private
    // TODO: other optional fields
}

impl MetaInfo {
    pub fn from_src_be(src: &[u8], mut be: BeValue) -> MiResult<Self> {
        let mut torrent = be.take_dict()?;

        let announce = torrent.expect("announce")?.take_str_utf8()?;

        let mut info = torrent.expect("info")?.take_dict()?;

        let piece_length = info.expect("piece length")?.take_uint()?;
        let total_length = info.expect("length")?.take_uint()?;
        let name = info.expect("name")?.take_str_utf8()?;
        let info_hash = {
            let info_slice = &src[info.src_range.clone()];
            get_hash(info_slice)
        };

        let piece_hashes = {
            let pieces = info.expect("pieces")?.take_str()?;

            if pieces.len() % 20 != 0 {
                return Err(MiError::InvalidPiecesLen(pieces.len()));
            }

            PieceHashes::new(pieces)
        };

        Ok(MetaInfo {
            info_hash,
            piece_length,
            total_length,
            name,
            piece_hashes,
            announce,
        })
    }
}

fn get_hash(src: &[u8]) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(src);
    hasher.finalize().to_vec()
}

#[derive(Debug)]
pub struct PieceHashes {
    hashes: Vec<u8>,
}

impl PieceHashes {
    pub fn new(hashes: Vec<u8>) -> Self {
        PieceHashes { hashes }
    }

    pub fn get_hash(&self, index: usize) -> Option<&[u8]> {
        let start = index * 20;
        self.hashes.get(start..start + 20)
    }
}

type MiResult<T> = Result<T, MiError>;

#[derive(Error, Debug)]
pub enum MiError {
    #[error("{0}")]
    BeError(#[from] ResponseParseError),
    #[error("Pieces length '{0}' should be a multiple of 20")]
    InvalidPiecesLen(usize),
}
