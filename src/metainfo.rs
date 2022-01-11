use sha1::{Digest, Sha1};
use thiserror::Error;

use crate::{
    bencoding::bevalue::{BeValue, ResponseParseError},
    piece_manager::PieceId,
};

#[derive(Debug)]
pub struct Metainfo {
    /// Tracker URL
    pub announce: String,
    /// Hash of the info dictionary
    pub info_hash: Vec<u8>,
    /// Number of bytes of 1 piece
    pub piece_length: u32,
    /// Length of the file in bytes (only single-file mode now)
    pub total_length: u64,
    /// Filename
    pub name: String,
    /// SHA1 hashes if the individual pieces
    pub piece_hashes: Vec<u8>,
    /// MD5 hash of the whole file
    pub md5sum: Option<Vec<u8>>,
}

impl Metainfo {
    pub fn from_src_be(src: &[u8], mut be: BeValue) -> MiResult<Self> {
        let torrent = be.get_dict()?;

        let announce = torrent.expect("announce")?.get_str_utf8()?;

        let info = torrent.expect("info")?.get_dict()?;

        let piece_length = info.expect("piece length")?.get_uint()? as u32;
        let name = info.expect("name")?.get_str_utf8()?;
        let info_hash = {
            let info_slice = &src[info.src_range.clone()];
            Self::sha1(info_slice)
        };

        let piece_hashes = info.expect("pieces")?.get_str()?.clone();
        if piece_hashes.len() % 20 != 0 {
            return Err(MiError::InvalidPiecesLen(piece_hashes.len()));
        }

        let file = if let Some(files) = info.get_mut("files") {
            let files = files.get_list()?;

            if files.len() > 1 {
                unimplemented!("Torrents with multiple files are unimplemented");
            }

            files[0].get_dict()?
        } else {
            info
        };

        let total_length = file.expect("length")?.get_uint()?;

        let md5sum = match file.get_mut("md5sum") {
            Some(md5) => Some(md5.get_str()?.clone()),
            None => None,
        };

        Ok(Metainfo {
            announce,
            info_hash,
            piece_length,
            total_length,
            name,
            piece_hashes,
            md5sum,
        })
    }

    /// Returns the hash of a specified piece
    pub fn get_hash(&self, index: PieceId) -> Option<&[u8]> {
        let start = index as usize * 20;
        self.piece_hashes.get(start..start + 20)
    }

    /// The number of pieces is 0-indexed !
    pub fn piece_count(&self) -> u32 {
        (self.total_length as f64 / self.piece_length as f64).ceil() as u32
    }

    /// Returns the piece size for a specific piece
    pub fn get_piece_size(&self, piece: PieceId) -> u32 {
        if piece != self.piece_count() - 1 {
            self.piece_length as u32
        } else {
            let prev_pieces_len = (self.piece_count() - 1) * self.piece_length;
            self.total_length as u32 - prev_pieces_len
        }
    }

    fn sha1(src: &[u8]) -> Vec<u8> {
        let mut hasher = Sha1::new();
        hasher.update(src);
        hasher.finalize().to_vec()
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
