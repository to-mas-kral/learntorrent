use std::{io::SeekFrom, sync::Arc};

use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt, BufWriter},
};

use crate::{metainfo::MetaInfo, p2p::CompletedBlockRequest, piece_manager::PieceId};

pub struct Io {
    rx: flume::Receiver<PieceIoMsg>,
    metainfo: Arc<MetaInfo>,
    counter: u32,
}

impl Io {
    pub fn new(metainfo: Arc<MetaInfo>) -> (Self, flume::Sender<PieceIoMsg>) {
        let (tx, rx) = flume::bounded(10000);

        let s = Self {
            rx,
            metainfo,
            counter: 0,
        };

        (s, tx)
    }

    pub async fn start(mut self) {
        let file = File::create(&self.metainfo.name)
            .await
            .expect("Failed to create the file");

        // I really don't know if bigger buffer sizes make sense
        let mut writer = BufWriter::with_capacity(10 * 1024 * 1024, file);

        loop {
            let piece_msg = self.rx.recv_async().await;
            match piece_msg {
                Ok(pm) => {
                    let piece_offset = pm.piece_id * self.metainfo.piece_length;
                    writer
                        .seek(SeekFrom::Start(piece_offset as u64))
                        .await
                        .expect("Seek to a wrong position");

                    for b in pm.completed_requests {
                        writer
                            .write_all_buf(&mut b.bytes.as_ref())
                            .await
                            .expect("IO error");
                    }

                    writer.flush().await.expect("IO error");

                    tracing::info!("Piece '{}' written to file", pm.piece_id);

                    self.counter += 1;
                    tracing::warn!("Pieces left: {}", self.metainfo.num_pieces() - self.counter);
                }
                Err(_) => break,
            }
        }
    }
}

pub struct PieceIoMsg {
    piece_id: PieceId,
    /// It is expected, that the blocks are sorted
    completed_requests: Vec<CompletedBlockRequest>,
}

impl PieceIoMsg {
    pub fn new(piece_id: PieceId, completed_requests: Vec<CompletedBlockRequest>) -> Self {
        Self {
            piece_id,
            completed_requests,
        }
    }
}
