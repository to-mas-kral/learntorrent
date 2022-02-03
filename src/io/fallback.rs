use std::{io::SeekFrom, sync::Arc};

use async_trait::async_trait;
use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt, BufWriter},
};

use super::{IoErr, PieceSave};
use crate::{metainfo::Metainfo, p2p::ValidatedPiece};

pub struct PieceSaver {
    metainfo: Arc<Metainfo>,
    writer: BufWriter<File>,
}

#[async_trait]
impl PieceSave for PieceSaver {
    async fn new(metainfo: Arc<Metainfo>) -> Result<Self, IoErr> {
        let file = File::create(&metainfo.name).await?;
        let writer = BufWriter::new(file);

        Ok(Self { metainfo, writer })
    }

    async fn on_piece_msg(&mut self, piece: ValidatedPiece) -> Result<(), IoErr> {
        let piece_offset = piece.pid * self.metainfo.piece_length;

        self.writer
            .seek(SeekFrom::Start(piece_offset as u64))
            .await?;

        for b in piece.completed_requests {
            self.writer.write_all_buf(&mut b.bytes.as_ref()).await?;
        }

        self.writer.flush().await?;

        tracing::debug!("Piece '{}' written to file", piece.pid);

        Ok(())
    }
}
