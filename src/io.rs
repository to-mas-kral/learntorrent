use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use bytes::BytesMut;
use thiserror::Error;
use tokio::{
    fs::{self, File},
    sync::mpsc,
};

use crate::{
    metainfo::{Metainfo, MultiFile, TorrentFileEntries},
    p2p::{CompletedBlockRequest, ValidatedPiece},
};

#[cfg(not(target_os = "linux"))]
mod fallback;
#[cfg(not(target_os = "linux"))]
use fallback::PieceSaver;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
use linux::PieceSaver;

/// Struct for saving the donwloaded pieces to disk.
pub struct Io {
    /// Receiver for the downloaded pieces
    piece_recv: mpsc::Receiver<IoMsg>,
    /// Platform-specific functionality
    piece_saver: PieceSaver,
    metainfo: Arc<Metainfo>,
}

impl Io {
    pub async fn new(
        piece_recv: mpsc::Receiver<IoMsg>,
        metainfo: Arc<Metainfo>,
    ) -> Result<Self, IoErr> {
        let files = match &metainfo.file_entries {
            TorrentFileEntries::Single(fe) => vec![File::create(&fe.name).await?],
            TorrentFileEntries::Multi(mf) => {
                let mut files = Vec::new();
                let base_path = PathBuf::from(&mf.dir_name);
                // TODO: support for multiple subdirectories (needs change in metainfo aswell)
                fs::create_dir_all(&base_path).await?;

                for fe in metainfo.file_entries.as_slice() {
                    let path = base_path.join(&fe.name);
                    files.push(File::create(path).await?);
                }

                files
            }
        };

        Ok(Self {
            piece_recv,
            piece_saver: PieceSaver::new(files).await?,
            metainfo,
        })
    }

    pub async fn start(mut self) -> Result<(), IoErr> {
        loop {
            match self.piece_recv.recv().await {
                Some(msg) => match msg {
                    IoMsg::Piece(piece) => {
                        let io_pieces = Self::split_piece(&self.metainfo, piece);

                        for piece in io_pieces {
                            self.piece_saver.on_piece_msg(piece).await?;
                        }
                    }
                    IoMsg::Exit => {
                        tracing::info!(
                            "Exiting - received the 'exit' message from the Piece Keeper"
                        );

                        return Ok(());
                    }
                },
                None => {
                    panic!("Internal error: I/O couldn't receive messages from the Piece Keeper")
                }
            }
        }
    }

    /// Converts between a ValidatedPiece and FileBlocks
    /// This is trivial in single-file mode
    /// In multi-file mode, we need to determine the span of the piece and cut it into parts if needed
    fn split_piece(metainfo: &Metainfo, piece: ValidatedPiece) -> Vec<IoPiece> {
        match &metainfo.file_entries {
            TorrentFileEntries::Single(_) => {
                let piece = IoPiece {
                    offset: piece.pid as u64 * metainfo.piece_length as u64,
                    blocks: piece.blocks,
                    file_index: 0,
                };

                vec![piece]
            }
            TorrentFileEntries::Multi(fe) => {
                let piece_start = piece.pid as u64 * metainfo.piece_length as u64;
                let piece_end = piece_start + metainfo.get_piece_size(piece.pid) as u64 - 1;

                let (start_i, start_fe) = fe
                    .file_entries
                    .iter()
                    .enumerate()
                    .find(|(_, fe)| fe.start <= piece_start && fe.end >= piece_start)
                    .expect("Internal error: I/O received an invalid piece");

                let (end_i, _) = fe
                    .file_entries
                    .iter()
                    .enumerate()
                    .find(|(_, fe)| fe.start <= piece_end && fe.end >= piece_end)
                    .expect("Internal error: I/O received an invalid piece");

                // Non-overlapping piece
                if start_i == end_i {
                    let piece = IoPiece {
                        offset: piece_start - start_fe.start,
                        blocks: piece.blocks,
                        file_index: start_i,
                    };

                    return vec![piece];
                } else if start_i < end_i {
                    Self::split_overlapping(fe, piece, start_i, end_i, piece_start, piece_end)
                } else {
                    unreachable!()
                }
            }
        }
    }

    /// Split a piece, that overlaps multiple files into separate parts
    fn split_overlapping(
        fe: &MultiFile,
        piece: ValidatedPiece,
        start_i: usize,
        end_i: usize,
        piece_start: u64,
        piece_end: u64,
    ) -> Vec<IoPiece> {
        tracing::error!("unimplemented - Piece spans multiple files");

        let mut io_pieces = vec![];

        // Flatten the vector of blocks into a contiguous buffer for an easier implementation
        let mut block = Vec::new();
        for cbr in piece.blocks {
            block.extend_from_slice(&cbr.bytes)
        }

        let mut block = &block[..];

        let mut current_pos = piece_start as usize;

        for fe_index in start_i..=end_i {
            let fe = &fe.file_entries[fe_index];

            let end = fe.end.min(piece_end);
            let len = (end as usize - current_pos) + 1;

            let io_buf = BytesMut::from(&block[..len]);
            let cbr = CompletedBlockRequest {
                offset: 0,
                size: io_buf.len() as u32,
                bytes: io_buf,
            };

            let io_piece = IoPiece {
                offset: current_pos as u64 - fe.start,
                blocks: vec![cbr],
                file_index: fe_index,
            };

            io_pieces.push(io_piece);

            block = &block[len..];
            current_pos += len;
        }

        io_pieces
    }
}

#[cfg_attr(test, derive(Debug, PartialEq))]
pub struct IoPiece {
    /// Offset of the piece in the specific file
    offset: u64,
    /// File data, blocks are sorted by offset
    blocks: Vec<CompletedBlockRequest>,
    /// Index of the file in the TorrentFiles struct
    file_index: usize,
}

#[async_trait]
pub trait PieceSave {
    async fn new(files: Vec<File>) -> Result<PieceSaver, IoErr>;
    async fn on_piece_msg(&mut self, piece: IoPiece) -> Result<(), IoErr>;
}

#[derive(Debug)]
pub enum IoMsg {
    /// A downloaded and valited piece that should be saved to disk
    Piece(ValidatedPiece),
    /// Notification from the PieceKeeper that no other pieces are coming
    Exit,
}

#[allow(dead_code)]
#[derive(Error, Debug)]
pub enum IoErr {
    #[error("IO error: '{0}")]
    Io(#[from] std::io::Error),
    #[error("Uring specific error: failed to write a piece to the file")]
    Uring,
}

#[cfg(test)]
mod test_piece_split {
    use crate::metainfo::{FileEntry, MultiFile};

    use super::*;

    #[rustfmt::skip]
    fn create_metainfo_short(piece_length: u32) -> Metainfo {
        let fe_0 = FileEntry { name: "".to_string(), start: 0, end: 127, len: 128 };
        let fe_1 = FileEntry { name: "".to_string(), start: 128, end: 191, len: 64 };
        let fe_2 = FileEntry { name: "".to_string(), start: 192, end: 223, len: 32 };

        let file_entries = vec![fe_0, fe_1, fe_2];

        create_mock_metainfo(piece_length, file_entries)
    }

    #[rustfmt::skip]
    fn create_metainfo_long(piece_length: u32) -> Metainfo {
        let fe_0 = FileEntry { name: "".to_string(), start: 0, end: 256, len: 257 };
        let fe_1 = FileEntry { name: "".to_string(), start: 257, end: 384, len: 128 };
        let fe_2 = FileEntry { name: "".to_string(), start: 385, end: 448, len: 64 };
        let fe_3 = FileEntry { name: "".to_string(), start: 449, end: 704, len: 256 };


        let file_entries = vec![fe_0, fe_1, fe_2, fe_3];

        create_mock_metainfo(piece_length, file_entries)
    }

    #[rustfmt::skip]
    fn create_mock_metainfo(piece_length: u32, file_entries: Vec<FileEntry>) -> Metainfo {
        let total_length = file_entries.iter().map(|fe| fe.len).sum();
        let multi_file = MultiFile {
            dir_name: "".to_string(),
            file_entries,
        };

        let file_entries = TorrentFileEntries::Multi(multi_file);

        Metainfo {
            trackers: vec![],
            info_hash: Box::new([0; 20]),
            piece_length,
            total_length,
            file_entries,
            piece_hashes: vec![],
        }
    }

    fn check_piece(metainfo: &Metainfo, pid: u32, offset: u64, file_index: usize) {
        let piece = ValidatedPiece {
            pid,
            blocks: vec![],
        };

        let pieces = Io::split_piece(metainfo, piece);
        let expected_first = IoPiece {
            offset,
            blocks: vec![],
            file_index,
        };

        assert_eq!(pieces.len(), 1);
        assert_eq!(pieces[0], expected_first);
    }

    #[test]
    fn test_split_non_overlapping() {
        let metainfo = create_metainfo_short(20);

        check_piece(&metainfo, 0, 0, 0);
        check_piece(&metainfo, 7, 12, 1);
        check_piece(&metainfo, 11, 28, 2);
    }

    #[test]
    fn test_split_non_overlapping_edge() {
        let metainfo = create_metainfo_short(32);

        check_piece(&metainfo, 4, 0, 1);
        check_piece(&metainfo, 5, 32, 1);
        check_piece(&metainfo, 6, 0, 2);
    }

    #[rustfmt::skip]
    #[test]
    fn test_split_overlapping_edge_start() {
        let file_entries = create_metainfo_short(128);

        let mut bytes = BytesMut::new();
        bytes.extend_from_slice(&vec![0; 64]);
        bytes.extend_from_slice(&vec![1; 32]);

        let cbr = CompletedBlockRequest { offset: 0, size: 96, bytes };
        let piece = ValidatedPiece { pid: 1, blocks: vec![cbr] };

        let pieces = Io::split_piece(&file_entries, piece);

        let bytes_1 = BytesMut::from_iter(vec![0u8; 64].iter());
        let cb_1 = CompletedBlockRequest { offset: 0, size: 64, bytes: bytes_1 };
        let expected_1 = IoPiece { offset: 0, blocks: vec![cb_1], file_index: 1 };

        let bytes_2 = BytesMut::from_iter(vec![1u8; 32].iter());
        let cb_2 = CompletedBlockRequest { offset: 0, size: 32, bytes: bytes_2 };
        let expected_2 = IoPiece { offset: 0, blocks: vec![cb_2], file_index: 2 };

        assert_eq!(&[expected_1, expected_2], pieces.as_slice());
    }

    #[rustfmt::skip]
    #[test]
    fn test_split_overlapping_edge_end() {
        let file_entries = create_metainfo_short(96);

        let mut bytes = BytesMut::new();
        bytes.extend_from_slice(&vec![0; 32]);
        bytes.extend_from_slice(&vec![1; 64]);

        let cbr = CompletedBlockRequest { offset: 0, size: 96, bytes };
        let piece = ValidatedPiece { pid: 1, blocks: vec![cbr] };

        let pieces = Io::split_piece(&file_entries, piece);

        let bytes_1 = BytesMut::from_iter(vec![0u8; 32].iter());
        let cb_1 = CompletedBlockRequest { offset: 0, size: 32, bytes: bytes_1 };
        let expected_1 = IoPiece { offset: 96, blocks: vec![cb_1], file_index: 0 };

        let bytes_2 = BytesMut::from_iter(vec![1u8; 64].iter());
        let cb_2 = CompletedBlockRequest { offset: 0, size: 64, bytes: bytes_2 };
        let expected_2 = IoPiece { offset: 0, blocks: vec![cb_2], file_index: 1 };

        assert_eq!(&[expected_1, expected_2], pieces.as_slice());
    }

    #[rustfmt::skip]
    #[test]
    fn test_split_overlapping_two() {
        let file_entries = create_metainfo_long(256);

        let mut bytes = BytesMut::new();
        bytes.extend_from_slice(&vec![0; 1]);
        bytes.extend_from_slice(&vec![1; 128]);
        bytes.extend_from_slice(&vec![2; 64]);
        bytes.extend_from_slice(&vec![3; 63]);

        let cbr = CompletedBlockRequest { offset: 0, size: 256, bytes };
        let piece = ValidatedPiece { pid: 1, blocks: vec![cbr] };

        let pieces = Io::split_piece(&file_entries, piece);

        let bytes_0 = BytesMut::from_iter(vec![0u8; 1].iter());
        let cb_0 = CompletedBlockRequest { offset: 0, size: 1, bytes: bytes_0 };
        let expected_0 = IoPiece { offset: 256, blocks: vec![cb_0], file_index: 0};

        let bytes_1 = BytesMut::from_iter(vec![1u8; 128].iter());
        let cb_1 = CompletedBlockRequest { offset: 0, size: 128, bytes: bytes_1 };
        let expected_1 = IoPiece { offset: 0, blocks: vec![cb_1], file_index: 1};

        let bytes_2 = BytesMut::from_iter(vec![2u8; 64].iter());
        let cb_2 = CompletedBlockRequest { offset: 0, size: 64, bytes: bytes_2 };
        let expected_2 = IoPiece { offset: 0, blocks: vec![cb_2], file_index: 2};

        let bytes_3 = BytesMut::from_iter(vec![3u8; 63].iter());
        let cb_3 = CompletedBlockRequest { offset: 0, size: 63, bytes: bytes_3 };
        let expected_3 = IoPiece { offset: 0, blocks: vec![cb_3], file_index: 3};

        assert_eq!(&[expected_0, expected_1, expected_2, expected_3], pieces.as_slice());
    }
}
