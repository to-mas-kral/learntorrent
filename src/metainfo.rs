use sha1::{Digest, Sha1};
use thiserror::Error;

use crate::{
    bencoding::bevalue::{BeValue, Dict, ResponseParseError},
    piece_keeper::PieceId,
};

pub struct Metainfo {
    /// Tracker URLs
    pub trackers: Vec<String>,
    /// Hash of the info dictionary
    pub info_hash: Box<[u8; 20]>,
    /// Number of bytes of 1 piece
    pub piece_length: u32,
    /// Length of the file in bytes (only single-file mode now)
    pub total_length: u64,
    /// Filename
    pub name: String,
    /// SHA1 hashes if the individual pieces
    pub piece_hashes: Vec<u8>,
}

impl Metainfo {
    pub fn from_src_be(src: &[u8], mut be: BeValue) -> MiResult<Self> {
        let torrent = be.get_dict()?;

        let trackers = Self::parse_trackers(torrent)?;

        let info = torrent.expect("info")?.get_dict()?;

        let piece_length = info.expect("piece length")?.get_u32()?;
        let name = info.expect("name")?.get_str_utf8()?;
        let info_hash = {
            let info_slice = &src[info.src_range.clone()];
            Self::sha1(info_slice)
        };

        let piece_hashes = info.expect("pieces")?.get_str()?;

        let file = if let Some(files) = info.get_mut("files") {
            let files = files.get_list()?;

            if files.len() > 1 {
                unimplemented!("Torrents with multiple files are unimplemented");
            }

            files[0].get_dict()?
        } else {
            info
        };

        let total_length = file.expect("length")?.get_u64()?;

        let mi = Metainfo {
            trackers,
            info_hash,
            piece_length,
            total_length,
            name,
            piece_hashes,
        };

        // Validate the length of the hashes string

        let len = mi.piece_hashes.len();
        if len % 20 != 0 || len / 20 != mi.piece_count() as usize {
            return Err(MiError::InvalidHashesLen(len));
        }

        Ok(mi)
    }

    // announce      = single URL
    // announce-list = list of lists URLs
    // url-list      = list of URLs
    fn parse_trackers(torrent: &mut Dict) -> Result<Vec<String>, MiError> {
        let mut trackers = torrent
            .try_get("announce", BeValue::get_str_utf8)?
            .and_then(|a| Some(vec![a]))
            .unwrap_or(vec![]);

        let announce_list = torrent.try_get_ref_mut("announce-list", BeValue::get_list)?;
        if let Some(announce_list) = announce_list {
            for l in announce_list {
                let url_list = l.get_list()?;
                let url = url_list
                    .get_mut(0)
                    .ok_or(MiError::MalformedAnnouceList)?
                    .get_str_utf8()?;

                trackers.push(url);
            }
        }

        let url_list = torrent.try_get_ref_mut("url-list", BeValue::get_list)?;
        if let Some(url_list) = url_list {
            for url in url_list {
                let url = url.get_str_utf8()?;
                trackers.push(url);
            }
        }

        Ok(trackers)
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

    fn sha1(src: &[u8]) -> Box<[u8; 20]> {
        let mut hasher = Sha1::new();
        hasher.update(src);

        let gen_arr = hasher.finalize();
        Box::new(gen_arr.into())
    }
}

impl std::fmt::Debug for Metainfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metainfo")
            .field("announce", &self.trackers)
            .field("info_hash", &self.info_hash)
            .field("piece_length", &self.piece_length)
            .field("total_length", &self.total_length)
            .field("name", &self.name)
            .field("piece_hashes", &format_args!("<piece hashes>"))
            .finish()
    }
}

type MiResult<T> = Result<T, MiError>;

#[derive(Error, Debug)]
pub enum MiError {
    #[error("{0}")]
    BeError(#[from] ResponseParseError),
    #[error("Hashes length '{0}' should be a multiple of 20")]
    InvalidHashesLen(usize),
    #[error("Announce list is malformed")]
    MalformedAnnouceList,
}
