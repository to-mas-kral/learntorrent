use std::io::SeekFrom;

use async_trait::async_trait;
use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt, BufWriter},
};

use super::{IoErr, IoPiece, PieceSave};

pub struct PieceSaver {
    writers: Vec<BufWriter<File>>,
}

#[async_trait]
impl PieceSave for PieceSaver {
    async fn new(files: Vec<File>) -> Result<Self, IoErr> {
        let writers = files.into_iter().map(|f| BufWriter::new(f)).collect();

        Ok(Self { writers })
    }

    async fn on_piece_msg(&mut self, piece: IoPiece) -> Result<(), IoErr> {
        let writer = &mut self.writers[piece.file_index];

        writer.seek(SeekFrom::Start(piece.offset as u64)).await?;

        for b in piece.blocks {
            writer.write_all_buf(&mut b.bytes.as_ref()).await?;
        }

        writer.flush().await?;

        tracing::debug!("Piece with offset '{}' written to file", piece.offset);

        Ok(())
    }
}
