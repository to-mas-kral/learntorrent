use bytes::BytesMut;
use sha1::{Digest, Sha1};

use super::PeerErr;
use crate::{metainfo::Metainfo, piece_manager::PieceId};

/// Tracks download progress of the current piece
pub struct PieceTracker {
    /// ID of the piece
    pub pid: PieceId,
    /// Size of the piece in bytes
    pub piece_size: u32,
    /// Marks the amount of bytes that have been downloaded already
    pub offset: u32,
    /// Pending *block* requests
    pub pending_requests: Vec<PendingBlockRequest>,
    /// Completed *block* requests
    pub completed_requests: Vec<CompletedBlockRequest>,
    /// Piece size - length of already downloaded blocks
    pub remaining_bytes: u32,
}

impl PieceTracker {
    pub const BLOCK_LEN: u32 = 16384;
    pub const MAX_PENDING_REQUESTS: usize = 5;

    pub fn new(piece_id: PieceId, piece_size: u32) -> Self {
        let pending_requests = Vec::with_capacity(Self::MAX_PENDING_REQUESTS);
        let completed_requests = Vec::with_capacity((piece_size / Self::BLOCK_LEN) as usize);

        Self {
            pid: piece_id,
            piece_size,
            offset: 0,
            pending_requests,
            completed_requests,
            remaining_bytes: piece_size,
        }
    }

    /// Calculates the offset and size of the next block
    pub fn next_pending_request(&mut self) -> Option<PendingBlockRequest> {
        let old_offset = self.offset;
        let remaining = self.piece_size - self.offset;

        if remaining > Self::BLOCK_LEN {
            self.offset += Self::BLOCK_LEN;
            return Some(PendingBlockRequest::new(old_offset, Self::BLOCK_LEN));
        }

        // Last block
        if remaining > 0 {
            self.offset += remaining;
            Some(PendingBlockRequest::new(old_offset, remaining))
        } else {
            None
        }
    }

    // TODO: smarter queueing strategy (based on peer speed)
    /// Queues new requests
    pub fn next_requests(&mut self) -> &[PendingBlockRequest] {
        let current_requests = self.pending_requests.len();
        let new_requests = Self::MAX_PENDING_REQUESTS - current_requests;

        if new_requests > 0 {
            for _ in 0..new_requests {
                if let Some(pr) = self.next_pending_request() {
                    self.pending_requests.push(pr)
                } else {
                    return &[];
                }
            }

            &self.pending_requests[current_requests..]
        } else {
            &[]
        }
    }

    /// Returns true if all blocks have been downloaded
    pub fn request_completed(&mut self, req: CompletedBlockRequest) -> Result<bool, PeerErr> {
        let index = self
            .pending_requests
            .iter()
            .position(|pr| pr.offset == req.offset && pr.size == req.size)
            .ok_or(PeerErr::InvalidBlockSize)?;
        self.pending_requests.remove(index);

        self.remaining_bytes -= req.size;
        self.completed_requests.push(req);

        Ok(self.remaining_bytes == 0)
    }

    /// Check the SHA1 sum of the piece and sort the blocks
    pub fn validate_piece(
        mut self,
        metainfo: &Metainfo,
    ) -> Result<Option<ValidatedPiece>, PeerErr> {
        let piece_hash = {
            self.completed_requests
                .sort_by(|a, b| a.offset.cmp(&b.offset));

            let mut hasher = Sha1::new();
            for b in &self.completed_requests {
                hasher.update(&b.bytes);
            }

            hasher.finalize()
        };

        // The length of the metainfo hash string must have been validated,
        // so it should contain all valid pieces
        // 'None' means that this task received an invalid piece from the PieceManager
        let expected_hash = metainfo
            .get_hash(self.pid)
            .ok_or(PeerErr::InvalidPiecePick)?;

        if expected_hash == piece_hash.as_slice() {
            Ok(Some(ValidatedPiece {
                pid: self.pid,
                completed_requests: self.completed_requests,
            }))
        } else {
            Ok(None)
        }
    }
}

pub struct PendingBlockRequest {
    pub offset: u32,
    pub size: u32,
}

impl PendingBlockRequest {
    pub fn new(offset: u32, len: u32) -> Self {
        Self { offset, size: len }
    }
}

pub struct CompletedBlockRequest {
    pub offset: u32,
    pub size: u32,
    pub bytes: BytesMut,
}

impl CompletedBlockRequest {
    pub fn new(offset: u32, len: u32, data: BytesMut) -> Self {
        Self {
            offset,
            size: len,
            bytes: data,
        }
    }
}

/// Created by calling 'validate()' on PieceTracker
/// The completed requests are sorted and validated
pub struct ValidatedPiece {
    /// ID of the piece
    pub pid: PieceId,
    /// Completed *block* requests
    pub completed_requests: Vec<CompletedBlockRequest>,
}
