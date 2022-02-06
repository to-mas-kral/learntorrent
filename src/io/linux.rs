use std::os::unix::prelude::AsRawFd;

use async_trait::async_trait;
use io_uring::{opcode, types, IoUring};
use tokio::fs::File;

use super::{IoErr, IoPiece, PieceSave};

pub struct PieceSaver {
    ring: IoUring,
    files: Vec<File>,
}

const RING_LEN: u32 = 1;

#[async_trait]
impl PieceSave for PieceSaver {
    async fn new(files: Vec<File>) -> Result<Self, IoErr> {
        Ok(Self {
            ring: IoUring::new(RING_LEN)?,
            files,
        })
    }

    async fn on_piece_msg(&mut self, piece: IoPiece) -> Result<(), IoErr> {
        let file = &self.files[piece.file_index];

        let iovecs: Vec<libc::iovec> = piece
            .blocks
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
        .offset(piece.offset as i64)
        .build()
        .user_data(piece.offset as u64);

        // INVESTIGATE: replace io_uring with an ordinary pwritev syscall since we are only
        // submitting 1 operation at a time

        // INVESTIGATE: might be a good idea to submit requests in bulk instead of one at a time,
        // although this might be inappropriate for streaming content (video, etc...)
        unsafe {
            // UNWRAP: queue can't be full if we always wait for the request to complete
            self.ring.submission().push(&write_sub).unwrap();
        }

        self.ring.submit_and_wait(1)?;

        // UNWRAP: queue should have at least one item because we just waited for completion
        let cqe = self.ring.completion().next().unwrap();

        if cqe.result() == -1 {
            return Err(IoErr::Uring);
        }

        tracing::debug!("Piece with offset '{}' written to file", cqe.user_data());

        Ok(())
    }
}
