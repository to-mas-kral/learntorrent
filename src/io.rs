use std::{io::SeekFrom, sync::Arc};

use thiserror::Error;
use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt, BufWriter},
};

use crate::{metainfo::Metainfo, p2p::ValidatedPiece};

/// Struct for saving the donwloaded pieces to disk.
/// This is curretnly implemented in a very simple way (doing a seek and a write for every piece).
/// I want to explore more more performant ways in the future.
pub struct Io {
    rx: flume::Receiver<ValidatedPiece>,
    metainfo: Arc<Metainfo>,
    missing_pieces: u32,
}

impl Io {
    pub fn new(metainfo: Arc<Metainfo>) -> (Self, flume::Sender<ValidatedPiece>) {
        let (tx, rx) = flume::bounded(100);

        let s = Self {
            rx,
            missing_pieces: metainfo.piece_count(),
            metainfo,
        };

        (s, tx)
    }

    pub async fn start(mut self) -> Result<(), IoErr> {
        let file = File::create(&self.metainfo.name).await?;

        // I really don't know if bigger buffer sizes make sense
        let mut writer = BufWriter::with_capacity(10 * 1024 * 1024, file);

        loop {
            if self.missing_pieces == 0 {
                tracing::info!("Exiting - all pieces have been saved to disk");
                return Ok(());
            }

            let piece_msg = self.rx.recv_async().await?;
            self.on_piece_msg(piece_msg, &mut writer).await?;
        }
    }

    async fn on_piece_msg(
        &mut self,
        pm: ValidatedPiece,
        writer: &mut BufWriter<File>,
    ) -> Result<(), IoErr> {
        let piece_offset = pm.pid * self.metainfo.piece_length;

        writer.seek(SeekFrom::Start(piece_offset as u64)).await?;

        for b in pm.completed_requests {
            writer.write_all_buf(&mut b.bytes.as_ref()).await?;
        }

        writer.flush().await?;

        tracing::debug!("Piece '{}' written to file", pm.pid);

        self.missing_pieces -= 1;

        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum IoErr {
    #[error("Channel error while receiving messages")]
    ChannelRecvErr(#[from] flume::RecvError),
    #[error("IO error: '{0}")]
    FileCreationErr(#[from] std::io::Error),
}
