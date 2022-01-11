mod message;

use std::{net::SocketAddrV4, sync::Arc, time::Duration};

use bytes::BytesMut;
use flume::{Receiver, Sender};
use futures::sink::SinkExt;
use futures_util::StreamExt;
use sha1::{Digest, Sha1};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::watch::Receiver as WReceiver,
    time::error::Elapsed,
};
use tokio_util::codec::Framed;

use crate::{
    io::PieceIoMsg,
    metainfo::Metainfo,
    piece_manager::{PieceId, PieceMsg, PmMsg, TaskId, TaskRegInfo, TorrentState},
};

use self::message::{Message, MessageCodec, MessageDecodeErr, MessageEncodeErr};

type MsgStream = Framed<TcpStream, MessageCodec>;

pub struct PeerTask {
    /// ID of the task assigned by Piece Manager
    id: TaskId,
    /// IPv4 address of the peer
    socket_addr: SocketAddrV4,
    /// The handshake shared for all peer connections
    handshake: Arc<Handshake>,
    /// Sender for communicating with Piece Manager
    pm_sender: Sender<PmMsg>,
    /// Receiver for communcating with Piece Manager
    piece_recv: Receiver<PieceMsg>,
    /// Receiver for broadcast notifications from the Piece Manager
    notify_recv: WReceiver<TorrentState>,

    /// Torrent metainfo
    metainfo: Arc<Metainfo>,

    am_choked: bool,
    am_interested: bool,

    /// Holds information about the progress of the currently downloaded piece
    piece_tracker: Option<PieceTracker>,
}

impl PeerTask {
    pub fn new(
        id: TaskId,
        socket_addr: SocketAddrV4,
        handshake: Arc<Handshake>,
        pm_sender: Sender<PmMsg>,
        piece_recv: Receiver<PieceMsg>,
        notify_recv: WReceiver<TorrentState>,
        metainfo: Arc<Metainfo>,
    ) -> Self {
        Self {
            id,
            socket_addr,
            handshake,
            pm_sender,
            piece_recv,
            notify_recv,
            metainfo,
            am_choked: true,
            am_interested: false,
            piece_tracker: None,
        }
    }

    pub async fn create(
        socket_addr: SocketAddrV4,
        handshake: Arc<Handshake>,
        metainfo: Arc<Metainfo>,
        pm_sender: Sender<PmMsg>,
        reg_recv: Receiver<TaskRegInfo>,
        notify_recv: WReceiver<TorrentState>,
    ) -> Result<(), PeerErr> {
        pm_sender
            .send_async(PmMsg::Register)
            .await
            .expect("Shouldn't panic ?");
        let task_reg_info = reg_recv.recv_async().await.expect("Shouldn't panic ?");

        let peer_task = PeerTask::new(
            task_reg_info.id,
            socket_addr,
            handshake,
            pm_sender,
            task_reg_info.piece_recv,
            notify_recv,
            metainfo,
        );

        peer_task.init().await
    }

    async fn init(mut self) -> Result<(), PeerErr> {
        let res = self.process().await;

        if res.is_err() {
            if let Some(pt) = &self.piece_tracker {
                self.pm_sender
                    .send_async(PmMsg::PieceFailed(pt.piece_id))
                    .await?;
                tracing::info!("failure while requesting piece '{}'", pt.piece_id);
            }
        }

        self.pm_sender
            .send_async(PmMsg::Deregister(self.id))
            .await
            .expect("Shouldn't panic ?");

        res
    }

    async fn process(&mut self) -> Result<(), PeerErr> {
        let mut msg_stream = self.setup_connection().await?;

        let mut first_message = true;
        loop {
            self.pick_piece().await?;
            self.queue_requests(&mut msg_stream).await?;
            self.send_interested(&mut msg_stream).await?;

            tokio::select! {
                val = self.notify_recv.changed() => {
                    val.expect("Shouldn't panic");

                    if let TorrentState::Complete = *self.notify_recv.borrow() {
                        tracing::debug!("Exiting - task '{}' received torrent completion notification", self.id);
                        return Ok(());
                    }
                }
                Ok(msg) = tokio::time::timeout(Duration::from_secs(60), async {
                    msg_stream.next().await
                }) => {
                    match msg {
                        None => return Err(PeerErr::Terminated),
                        Some(m) => {
                            self.on_msg_received(m?, first_message).await?;
                            first_message = false;
                        }
                    }
                }
            }
        }
    }

    async fn on_msg_received(&mut self, msg: Message, first_message: bool) -> Result<(), PeerErr> {
        match msg {
            Message::KeepAlive => tracing::debug!("Peer '{}' keep-alive", self.id),
            Message::Choke => self.on_choke().await?,
            Message::Unchoke => self.on_unchoke(),
            Message::Have(piece_id) => self.on_have(piece_id).await?,
            Message::Bitfield(bitfield) => self.on_bitfield(first_message, bitfield).await?,
            Message::Piece {
                index,
                begin,
                block,
            } => self.on_block_receive(index, begin, block).await?,
            m => tracing::warn!("Unimplmeneted message received: {:?}", m),
        }

        Ok(())
    }

    fn on_unchoke(&mut self) {
        tracing::debug!("Peer '{}' unchoked", self.id);
        self.am_choked = false;
    }

    async fn on_bitfield(
        &mut self,
        first_message: bool,
        bitfield: BytesMut,
    ) -> Result<(), PeerErr> {
        if first_message {
            tracing::debug!("Peer '{}' has a bitfield", self.id);
            // TODO: verify the bitfield length
            self.pm_sender
                .send_async(PmMsg::Bitfield(self.id, bitfield))
                .await?
        } else {
            return Err(PeerErr::BitfieldNotFirst);
        }

        Ok(())
    }

    async fn on_have(&mut self, piece_id: u32) -> Result<(), PeerErr> {
        tracing::debug!("Peer '{}' has new piece '{}'", self.id, piece_id);

        self.pm_sender
            .send_async(PmMsg::Have(self.id, piece_id))
            .await?;

        Ok(())
    }

    async fn on_choke(&mut self) -> Result<(), PeerErr> {
        tracing::debug!("Peer '{}' choked", self.id);

        self.am_choked = true;

        if let Some(pt) = &self.piece_tracker {
            self.pm_sender
                .send_async(PmMsg::PieceFailed(pt.piece_id))
                .await?;
        }

        self.piece_tracker = None;
        Ok(())
    }

    async fn on_block_receive(
        &mut self,
        index: u32,
        begin: u32,
        block: BytesMut,
    ) -> Result<(), PeerErr> {
        tracing::trace!("Task '{}' received a block", self.id);

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
            tracing::debug!("Piece '{}' finished", pt.piece_id);

            let io_msg = PieceIoMsg::new(pt.piece_id, pt.completed_requests);
            self.pm_sender
                .send_async(PmMsg::PieceFinished(io_msg))
                .await?;
        } else {
            tracing::info!("Piece '{}' bad hash", pt.piece_id);

            self.pm_sender
                .send_async(PmMsg::PieceFailed(pt.piece_id))
                .await?;
        }

        Ok(())
    }

    fn validate_piece(pt: &mut PieceTracker, metainfo: &Metainfo) -> bool {
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
    async fn pick_piece(&mut self) -> Result<(), PeerErr> {
        if !self.am_choked && self.piece_tracker.is_none() {
            self.pm_sender.send_async(PmMsg::Pick(self.id)).await?;
            let piece_msg = self.piece_recv.recv_async().await?;

            if let PieceMsg::Piece(pid) = piece_msg {
                tracing::debug!("Task '{}' picked piece '{}'", self.id, pid);

                let piece_size = self.metainfo.get_piece_size(pid);
                self.piece_tracker = Some(PieceTracker::new(pid, piece_size));
            }
        }

        Ok(())
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
            std::time::Duration::from_secs(30),
            TcpStream::connect(self.socket_addr),
        )
        .await??;

        tracing::debug!(
            "Successfull connection: '{:?}'. Sending a handshake.",
            self.socket_addr
        );

        stream.write_all(&self.handshake.inner).await?;

        let mut peer_handshake = Handshake::new_empty();
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            stream.read_exact(&mut peer_handshake.inner).await
        })
        .await??;

        let _peer_id = peer_handshake.validate(&peer_handshake)?;
        let msg_stream = MsgStream::new(stream, MessageCodec);

        tracing::debug!("Handshake with '{:?}' complete", self.socket_addr);

        Ok(msg_stream)
    }
}

#[derive(Error, Debug)]
pub enum PeerErr {
    #[error("TCP error: '{0}'")]
    FailedConnection(#[from] tokio::io::Error),

    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("The session was terminated by the peer")]
    Terminated,
    #[error("Received an invalid handshake")]
    InvalidHandshake,
    #[error("Received a block related to an invalid piece")]
    InvalidPieceReceived,
    #[error("Received a block of invalid size")]
    InvalidBlockSize,
    #[error("'Bitfield' message wasn't the first message")]
    BitfieldNotFirst,

    #[error("Message decoding error: '{0}'")]
    InvalidMessage(#[from] MessageDecodeErr),
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

    pub fn new(client_id: &[u8], metainfo: &Metainfo) -> Self {
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
