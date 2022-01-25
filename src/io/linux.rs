use std::{os::unix::prelude::AsRawFd, sync::Arc};

use io_uring::{opcode, types, IoUring};
use thiserror::Error;
use tokio::fs::File;

use crate::{metainfo::Metainfo, p2p::ValidatedPiece};

/// Struct for saving the donwloaded pieces to disk.
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

    const RING_LEN: u32 = 64;

    pub async fn start(mut self) -> Result<(), IoErr> {
        let file = File::create(&self.metainfo.name).await?;

        let mut ring = IoUring::new(Self::RING_LEN)?;

        loop {
            if self.missing_pieces == 0 {
                tracing::info!("Exiting - all pieces have been saved to disk");
                return Ok(());
            }

            let piece_msg = self.rx.recv_async().await?;
            self.on_piece_msg(piece_msg, &mut ring, &file).await?;
        }
    }

    async fn on_piece_msg(
        &mut self,
        pm: ValidatedPiece,
        ring: &mut IoUring,
        file: &File,
    ) -> Result<(), IoErr> {
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

        unsafe {
            // UNWRAP: queue can't be full if we always wait for the request to complete
            ring.submission().push(&write_sub).unwrap();
        }

        ring.submit_and_wait(1)?;

        let cqe = ring.completion().next().expect("completion queue is empty");

        if cqe.result() == -1 {
            return Err(IoErr::UringErr);
        }

        tracing::debug!("Piece '{}' written to file", cqe.user_data());

        self.missing_pieces -= 1;

        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum IoErr {
    #[error("Channel error while receiving messages from Piece Manager")]
    ChannelRecvErr(#[from] flume::RecvError),
    #[error("IO error: '{0}")]
    IoErr(#[from] std::io::Error),
    #[error("Failed to write a piece to the file, errno")]
    UringErr,
}
