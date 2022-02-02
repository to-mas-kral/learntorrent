use std::{os::unix::prelude::AsRawFd, sync::Arc};

use io_uring::{opcode, types, IoUring};
use thiserror::Error;
use tokio::{
    fs::File,
    sync::{mpsc, watch},
};

use crate::{metainfo::Metainfo, p2p::ValidatedPiece, AppState};

/// Struct for saving the donwloaded pieces to disk.
pub struct Io {
    /// Receiver for downloaded and verified pieces
    piece_recv: mpsc::Receiver<IoMsg>,
    /// Receiver for AppState notifications
    appstate_recv: watch::Receiver<AppState>,
    /// Torrent metainfo
    metainfo: Arc<Metainfo>,
    /// The number of pieces not yet saved to disk
    missing_pieces: u32,

    ring: IoUring,
}

impl Io {
    pub fn new(
        piece_recv: mpsc::Receiver<IoMsg>,
        appstate_recv: watch::Receiver<AppState>,
        metainfo: Arc<Metainfo>,
    ) -> Result<Self, IoErr> {
        Ok(Self {
            piece_recv,
            appstate_recv,
            missing_pieces: metainfo.piece_count(),
            metainfo,
            ring: IoUring::new(Self::RING_LEN)?,
        })
    }

    const RING_LEN: u32 = 64;

    pub async fn start(mut self) -> Result<(), IoErr> {
        let file = File::create(&self.metainfo.name).await?;

        loop {
            tokio::select! {
                // If the pice_sender dropped, then we should either receive an appstate
                // exit notification or the Piece Keeper panicked, in which case we should
                // panic as well
                Some(IoMsg::Piece(piece)) = self.piece_recv.recv() => {
                    self.on_piece_msg(piece, &file).await?;

                    if self.missing_pieces == 0 {
                        tracing::info!("All pieces have been saved to disk");
                    }
                }
                appstate_notif = self.appstate_recv.changed() => {
                    appstate_notif
                    .expect("Internal error: I/O couldn't receive appstate notificatoin from the Main Task");

                    let mut should_exit = false;
                    if let AppState::Exit = *self.appstate_recv.borrow() {
                        tracing::info!("I/O received the app exit message");
                        should_exit = true;
                    }

                    if should_exit {
                        self.process_pending_pieces(&file).await?;
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn process_pending_pieces(&mut self, file: &File) -> Result<(), IoErr> {
        while let Some(io_msg) = self.piece_recv.recv().await {
            match io_msg {
                IoMsg::Piece(piece) => self.on_piece_msg(piece, file).await?,
                IoMsg::Exit => break,
            }
        }

        if self.missing_pieces == 0 {
            tracing::info!("All pieces have been saved to disk");
        }

        tracing::info!("Exiting - all pending pieces have been processed by the I/O task");

        Ok(())
    }

    async fn on_piece_msg(&mut self, pm: ValidatedPiece, file: &File) -> Result<(), IoErr> {
        let piece_offset = pm.pid * self.metainfo.piece_length;

        let iovecs: Vec<libc::iovec> = pm
            .completed_requests
            .iter()
            .map(|cr| libc::iovec {
                iov_base: cr.bytes.as_ptr() as _,
                iov_len: cr.size as usize,
            })
            .collect();

        let write_sub = opcode::Writev::new(
            types::Fd(file.as_raw_fd()),
            iovecs.as_ptr(),
            iovecs.len() as u32,
        )
        .offset(piece_offset as i64)
        .build()
        .user_data(pm.pid as u64);

        // TODO: should submit requests in bulk instead of one at a time
        unsafe {
            // UNWRAP: queue can't be full if we always wait for the request to complete
            self.ring.submission().push(&write_sub).unwrap();
        }

        self.ring.submit_and_wait(1)?;
        let cqe = self
            .ring
            .completion()
            .next()
            .expect("completion queue is empty");

        if cqe.result() == -1 {
            return Err(IoErr::Uring);
        }

        tracing::debug!("Piece '{}' written to file", cqe.user_data());

        self.missing_pieces -= 1;

        Ok(())
    }
}

#[derive(Debug)]
pub enum IoMsg {
    /// A downloaded and valited piece that should be saved to disk
    Piece(ValidatedPiece),
    /// Notification from the PieceKeeper that no other pieces are coming
    Exit,
}

#[derive(Error, Debug)]
pub enum IoErr {
    #[error("IO error: '{0}")]
    Io(#[from] std::io::Error),
    #[error("Failed to write a piece to the file, errno")]
    Uring,
}
