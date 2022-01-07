mod message;

use std::{net::SocketAddrV4, sync::Arc, time::Duration};

use bytes::BytesMut;
use futures::sink::SinkExt;
use futures_util::StreamExt;
use sha1::{Digest, Sha1};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::error::Elapsed,
};
use tokio_util::codec::Framed;

use crate::{
    io::PieceIoMsg,
    metainfo::MetaInfo,
    piece_manager::{PieceId, PieceMsg, PmMsg, TaskId},
};

use self::message::{Message, MessageCodec, MessageDecoderErr, MessageEncodeErr};

type MsgStream = Framed<TcpStream, MessageCodec>;

pub struct PeerTask {
    id: TaskId,
    socket_addr: SocketAddrV4,
    handshake: Arc<Handshake>,
    pm_tx: flume::Sender<PmMsg>,
    piece_rx: flume::Receiver<PieceMsg>,
    metainfo: Arc<MetaInfo>,
    io_tx: flume::Sender<PieceIoMsg>,

    am_choked: bool,
    am_interested: bool,

    piece_tracker: Option<PieceTracker>,
}

impl PeerTask {
    pub fn new(
        id: TaskId,
        socket_addr: SocketAddrV4,
        handshake: Arc<Handshake>,
        pm_sender: flume::Sender<PmMsg>,
        piece_rx: flume::Receiver<PieceMsg>,
        io_tx: flume::Sender<PieceIoMsg>,
        metainfo: Arc<MetaInfo>,
    ) -> Self {
        Self {
            id,
            socket_addr,
            handshake,
            pm_tx: pm_sender,
            piece_rx,
            metainfo,
            io_tx,
            am_choked: true,
            am_interested: false,
            piece_tracker: None,
        }
    }

    pub async fn create(mut self) -> Result<(), PeerErr> {
        match self.process().await {
            Ok(_) => Ok(()),
            Err(e) => {
                if let Some(pt) = &self.piece_tracker {
                    self.pm_tx
                        .send_async(PmMsg::PieceRestart(pt.piece_id))
                        .await?;
                    tracing::info!("failure while requesting piece '{}'", pt.piece_id);
                }

                Err(e)
            }
        }
    }

    async fn process(&mut self) -> Result<(), PeerErr> {
        let mut msg_stream = self.setup_connection().await?;

        let mut first_message = true;
        loop {
            if let None = self.pick_piece().await? {
                // All blocks have been requested
                // TODO: the correct mechanism should be waiting until all
                // pieces have been actually downloaded, verified and sent to disk
                return Ok(());
            }

            self.queue_requests(&mut msg_stream).await?;
            self.send_interested(&mut msg_stream).await?;

            // A bitfield message may only be sent immediately after the handshake
            let msg = Self::receive_message(&mut msg_stream, 120).await?;

            match msg {
                Message::KeepAlive => continue,
                Message::Choke => {
                    self.am_choked = true;

                    if let Some(pt) = &self.piece_tracker {
                        self.pm_tx
                            .send_async(PmMsg::PieceRestart(pt.piece_id))
                            .await?;
                        tracing::info!("Got choked while requesting a piece '{}'", pt.piece_id);
                    }

                    self.piece_tracker = None;
                }
                Message::Unchoke => self.am_choked = false,
                Message::Have(piece_id) => {
                    self.pm_tx
                        .send_async(PmMsg::Have(self.id, piece_id))
                        .await?;
                }
                Message::Bitfield(bitfield) => {
                    if first_message {
                        // TODO: verify the bitfield (length)
                        self.pm_tx
                            .send_async(PmMsg::HaveBitfield(self.id, bitfield))
                            .await?
                    } else {
                        // TODO: drop the connection after receiving bitfield as a non-first message
                    }
                }
                Message::Piece {
                    index,
                    begin,
                    block,
                } => self.on_block_receive(index, begin, block).await?,
                m => tracing::warn!("Unimplmeneted message received: {:?}", m),
            }

            first_message = false;
        }
    }

    async fn on_block_receive(
        &mut self,
        index: u32,
        begin: u32,
        block: BytesMut,
    ) -> Result<(), PeerErr> {
        if let Some(pt) = &mut self.piece_tracker {
            if index != pt.piece_id {
                return Err(PeerErr::InvalidPieceReceived);
            }

            let br = CompletedBlockRequest::new(begin, block.len() as u32, block);
            if !pt.request_completed(br)? {
                return Ok(());
            }
        } else {
            return Ok(());
        }

        // Only if whole piece is downloaded
        let mut pt = std::mem::take(&mut self.piece_tracker).unwrap();
        let valid = Self::validate_piece(&mut pt, &self.metainfo);

        if valid {
            tracing::info!("Piece '{}' successful", pt.piece_id);

            let io_msg = PieceIoMsg::new(pt.piece_id, pt.completed_requests);
            self.io_tx.send_async(io_msg).await?;
        } else {
            tracing::info!("Piece '{}' bad hash", pt.piece_id);

            self.pm_tx
                .send_async(PmMsg::PieceRestart(pt.piece_id))
                .await?;
        }

        Ok(())
    }

    fn validate_piece(pt: &mut PieceTracker, metainfo: &MetaInfo) -> bool {
        let piece_hash = {
            pt.completed_requests
                .sort_by(|a, b| a.offset.cmp(&b.offset));

            let mut hasher = Sha1::new();
            for b in &pt.completed_requests {
                hasher.update(&b.bytes);
            }

            hasher.finalize()
        };

        let expected_hash = metainfo.get_hash(pt.piece_id).expect("Shouldn't happen");
        expected_hash == piece_hash.as_slice()
    }

    /// If we are currently downloading a piece, ask the peer for some new
    /// blocks if needed
    async fn queue_requests(&mut self, msg_stream: &mut MsgStream) -> Result<(), PeerErr> {
        if let Some(pt) = &mut self.piece_tracker {
            let piece_id = pt.piece_id;
            for r in pt.next_requests() {
                msg_stream
                    .send(Message::Request {
                        index: piece_id,
                        begin: r.offset,
                        len: r.size,
                    })
                    .await?
            }
        }

        Ok(())
    }

    /// If we aren't choked by the peer and we aren't currently downloading a piece,
    /// ask the PieceManager for a new one
    async fn pick_piece(&mut self) -> Result<Option<()>, PeerErr> {
        if !self.am_choked && self.piece_tracker.is_none() {
            self.pm_tx.send_async(PmMsg::Pick(self.id)).await?;
            let piece_id = self.piece_rx.recv_async().await?;

            match piece_id {
                Some(pid) => {
                    tracing::info!("Task '{}' received piece '{}'", self.id, pid);

                    let piece_size = self.metainfo.get_piece_size(pid);
                    self.piece_tracker = Some(PieceTracker::new(pid, piece_size));
                }
                None => return Ok(None),
            }
        }

        Ok(Some(()))
    }

    /// If we are choked and not interested, send an Interested message to the peer
    async fn send_interested(&mut self, msg_stream: &mut MsgStream) -> Result<(), PeerErr> {
        if !self.am_interested && self.am_choked {
            self.am_interested = true;
            msg_stream.send(Message::Interested).await?;
        }

        Ok(())
    }

    async fn setup_connection(&mut self) -> Result<MsgStream, PeerErr> {
        let mut stream = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            TcpStream::connect(self.socket_addr),
        )
        .await??;

        tracing::trace!(
            "Successfull connection: '{:?}'. Sending a handshake.",
            self.socket_addr
        );

        stream.write_all(&self.handshake.inner).await?;

        let mut peer_handshake = Handshake::new_empty();
        tokio::time::timeout(std::time::Duration::from_secs(120), async {
            stream.read_exact(&mut peer_handshake.inner).await
        })
        .await??;

        let _peer_id = peer_handshake.validate(&peer_handshake)?;
        let msg_stream = MsgStream::new(stream, MessageCodec);

        tracing::info!("Handshake with '{:?}' complete", self.socket_addr);

        Ok(msg_stream)
    }

    async fn receive_message(msg_stream: &mut MsgStream, timeout: u64) -> Result<Message, PeerErr> {
        let res = tokio::time::timeout(Duration::from_secs(timeout), async {
            loop {
                let res = msg_stream.next().await;
                if let Some(inner) = res {
                    break inner;
                }
            }
        })
        .await;

        Ok(res??)
    }
}

#[derive(Error, Debug)]
pub enum PeerErr {
    #[error("TCP error: '{0}'")]
    FailedConnection(#[from] tokio::io::Error),

    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("Received an invalid handshake")]
    InvalidHandshake,
    #[error("Received a block related to an invalid piece")]
    InvalidPieceReceived,
    #[error("Received a block of invalid size")]
    InvalidBlockSize,

    #[error("Message decoding error: '{0}'")]
    InvalidMessage(#[from] MessageDecoderErr),
    #[error("Message encoding error: '{0}'")]
    MessageEncodeErr(#[from] MessageEncodeErr),

    #[error("Flume error")]
    FlumeErr(#[from] flume::SendError<PmMsg>),
    #[error("Flume error")]
    FlumeErr2(#[from] flume::RecvError),
    #[error("Flume error")]
    FlumeErr3(#[from] flume::SendError<PieceIoMsg>),
}

/// Tracks download progress of the current piece
struct PieceTracker {
    piece_id: PieceId,
    piece_size: u32,
    /// Marks the amount of bytes that have been downloaded already
    offset: u32,
    pending_requests: Vec<PendingBlockRequest>,
    completed_requests: Vec<CompletedBlockRequest>,
    /// Piece size - length of already downloaded blocks
    remaining_bytes: u32,
}

impl PieceTracker {
    const BLOCK_LEN: u32 = 16384;
    const MAX_PENDING_REQUESTS: usize = 5;

    fn new(piece_id: PieceId, piece_size: u32) -> Self {
        let pending_requests = Vec::with_capacity(Self::MAX_PENDING_REQUESTS);
        let completed_requests = Vec::with_capacity(32);

        Self {
            piece_id,
            piece_size,
            offset: 0,
            pending_requests,
            completed_requests,
            remaining_bytes: piece_size,
        }
    }

    fn next_pending_request(&mut self) -> Option<PendingBlockRequest> {
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

    fn next_requests(&mut self) -> &[PendingBlockRequest] {
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
    fn request_completed(&mut self, req: CompletedBlockRequest) -> Result<bool, PeerErr> {
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
}

#[derive(Debug)]
struct PendingBlockRequest {
    offset: u32,
    size: u32,
}

impl PendingBlockRequest {
    fn new(offset: u32, len: u32) -> Self {
        Self { offset, size: len }
    }
}

#[derive(Debug)]
pub struct CompletedBlockRequest {
    pub offset: u32,
    size: u32,
    pub bytes: BytesMut,
}

impl CompletedBlockRequest {
    fn new(offset: u32, len: u32, data: BytesMut) -> Self {
        Self {
            offset,
            size: len,
            bytes: data,
        }
    }
}

pub struct Handshake {
    pub inner: Vec<u8>,
}

impl Handshake {
    const HANDSHAKE_LEN: usize = 68;

    pub fn new(client_id: &[u8], metainfo: &MetaInfo) -> Self {
        let mut handshake = vec![0; Self::HANDSHAKE_LEN];
        handshake[0] = 0x13;
        handshake[1..20].copy_from_slice("BitTorrent protocol".as_bytes());
        // Extensions, currently all 0
        handshake[20..28].fill(0);
        handshake[28..48].copy_from_slice(&metainfo.info_hash);
        handshake[48..68].copy_from_slice(client_id);

        Handshake { inner: handshake }
    }

    pub fn new_empty() -> Self {
        Handshake {
            inner: vec![0; Self::HANDSHAKE_LEN],
        }
    }

    /// Check if info hash matches, then returns the peer id
    pub fn validate<'p>(&self, peer: &'p Self) -> Result<&'p [u8], PeerErr> {
        if self.inner[0..48] != peer.inner[0..48] {
            return Err(PeerErr::InvalidHandshake);
        }

        Ok(&peer.inner[48..68])
    }
}
