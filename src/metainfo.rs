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
    /// Total length of the file in bytes
    pub total_length: u64,
    /// Filenames, paths, lengths and offsets
    pub file_entries: TorrentFileEntries,
    /// SHA1 hashes of the individual pieces
    pub piece_hashes: Vec<[u8; 20]>,
}

impl Metainfo {
    pub fn from_src_be(src: &[u8], mut be: BeValue) -> MiResult<Self> {
        let torrent = be.get_dict()?;

        let trackers = Self::parse_trackers(torrent)?;

        let info = torrent.expect("info")?.get_dict()?;

        let piece_length = info.expect("piece length")?.get_u32()?;
        let info_hash = {
            let info_slice = &src[info.src_range.clone()];
            Self::sha1(info_slice)
        };

        let file_entries = Self::parse_files(info)?;
        let total_length = match &file_entries {
            TorrentFileEntries::Single(f) => f.len,
            TorrentFileEntries::Multi(mf) => mf.file_entries.iter().map(|f| f.len).sum(),
        };

        let piece_hashes = info.expect("pieces")?.get_str()?;

        let len = piece_hashes.len();
        if len % 20 != 0 {
            return Err(MiErr::InvalidHashesLen(len));
        }

        // TODO(nightly): array_chunks - https://dev-doc.rust-lang.org/std/slice/struct.ArrayChunks.html
        let piece_hashes: Vec<[u8; 20]> = piece_hashes
            .chunks(20)
            .map(|s| {
                let mut a = [0; 20];
                a.copy_from_slice(s);
                a
            })
            .collect();

        let mi = Metainfo {
            trackers,
            info_hash,
            piece_length,
            total_length,
            file_entries,
            piece_hashes,
        };

        // Validate hash count
        if mi.piece_hashes.len() != mi.piece_count() as usize {
            return Err(MiErr::InvalidHashesLen(len));
        }

        Ok(mi)
    }

    // announce      = single URL
    // announce-list = list of lists URLs
    // url-list      = list of URLs
    fn parse_trackers(torrent: &mut Dict) -> Result<Vec<String>, MiErr> {
        let mut trackers = torrent
            .try_get("announce", BeValue::get_str_utf8)?
            .and_then(|a| Some(vec![a]))
            .unwrap_or(vec![]);

        let announce_list = torrent.try_get_ref_mut("announce-list", BeValue::get_list)?;
        if let Some(announce_list) = announce_list {
            for l in announce_list {
                let url_list = l.get_list()?;

                for url in url_list {
                    trackers.push(url.get_str_utf8()?);
                }
            }
        }

        // TODO: some torrents do not use 'url-list' for tracker URLs, but rather for file URLs
        let url_list = torrent.try_get_ref_mut("url-list", BeValue::get_list)?;
        if let Some(url_list) = url_list {
            for url in url_list {
                let url = url.get_str_utf8()?;
                trackers.push(url);
            }
        }

        Ok(trackers)
    }

    fn parse_files(info: &mut Dict) -> Result<TorrentFileEntries, MiErr> {
        match info.contains("files") {
            // Multi-file
            true => {
                let dir_name = info.expect("name")?.get_str_utf8()?;
                let file_list = info.expect("files")?.get_list()?;

                let mut file_offset = 0;

                let mut file_entries = Vec::new();
                for file in file_list {
                    let file = file.get_dict()?;

                    let len = file.expect("length")?.get_u64()?;

                    let path = file.expect("path")?.get_list()?;
                    if path.len() > 1 {
                        unimplemented!("Multi-part file paths are unimplemented");
                    }

                    let path = match path.get_mut(0) {
                        Some(p) => p.get_str_utf8()?,
                        None => return Err(MiErr::InvalidFilePath),
                    };

                    let file_entry = FileEntry {
                        name: path,
                        len,
                        start: file_offset,
                        end: file_offset + len - 1,
                    };
                    file_entries.push(file_entry);

                    file_offset += len;
                }

                Ok(TorrentFileEntries::Multi(MultiFile {
                    dir_name,
                    file_entries,
                }))
            }
            // Single-file
            false => {
                let name = info.expect("name")?.get_str_utf8()?;
                let len = info.expect("length")?.get_u64()?;

                return Ok(TorrentFileEntries::Single(FileEntry {
                    name,
                    len,
                    start: 0,
                    end: len,
                }));
            }
        }
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
            .field("files", &self.file_entries)
            .field("piece_hashes", &format_args!("<piece hashes>"))
            .finish()
    }
}

#[derive(Debug)]
pub enum TorrentFileEntries {
    Single(FileEntry),
    Multi(MultiFile),
}

impl TorrentFileEntries {
    pub fn as_slice(&self) -> &[FileEntry] {
        match self {
            TorrentFileEntries::Single(fe) => std::slice::from_ref(fe),
            TorrentFileEntries::Multi(mf) => &mf.file_entries,
        }
    }
}

#[derive(Debug)]
pub struct MultiFile {
    pub dir_name: String,
    pub file_entries: Vec<FileEntry>,
}

#[derive(Debug)]
pub struct FileEntry {
    pub name: String,
    pub len: u64,
    pub start: u64,
    pub end: u64,
}

type MiResult<T> = Result<T, MiErr>;

#[derive(Error, Debug)]
pub enum MiErr {
    #[error("{0}")]
    BeError(#[from] ResponseParseError),
    #[error("Hashes length '{0}' should be a multiple of 20")]
    InvalidHashesLen(usize),
    #[error("The 'info' dict contains an invalid file path")]
    InvalidFilePath,
}
