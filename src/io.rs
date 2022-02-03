use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::{metainfo::Metainfo, p2p::ValidatedPiece};

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
}

impl Io {
    pub async fn new(
        piece_recv: mpsc::Receiver<IoMsg>,
        metainfo: Arc<Metainfo>,
    ) -> Result<Self, IoErr> {
        Ok(Self {
            piece_recv,
            piece_saver: PieceSaver::new(metainfo).await?,
        })
    }

    pub async fn start(mut self) -> Result<(), IoErr> {
        loop {
            match self.piece_recv.recv().await {
                Some(msg) => match msg {
                    IoMsg::Piece(piece) => self.piece_saver.on_piece_msg(piece).await?,
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
}

#[async_trait]
pub trait PieceSave {
    async fn new(metainfo: Arc<Metainfo>) -> Result<PieceSaver, IoErr>;
    async fn on_piece_msg(&mut self, piece: ValidatedPiece) -> Result<(), IoErr>;
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
