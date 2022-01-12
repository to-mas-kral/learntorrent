use std::{net::SocketAddrV4, sync::Arc, time::Duration};

use bytes::BytesMut;
use flume::{Receiver, Sender};
use futures::sink::SinkExt;
use futures_util::StreamExt;
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::watch::Receiver as WReceiver,
};
use tokio_util::codec::Framed;

use crate::{
    metainfo::Metainfo,
    p2p::piece_tracker::{CompletedBlockRequest, PieceTracker},
    piece_manager::{PieceMsg, PmMsg, TaskId, TaskRegInfo, TorrentState},
};

use self::message::{Message, MessageCodec, MessageDecodeErr, MessageEncodeErr};

mod message;
mod piece_tracker;

pub use piece_tracker::ValidatedPiece;

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

    // Is this task choked by the peer ?
    am_choked: bool,
    // Has this task already expressed interest in receiving pieces ?
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
        pm_sender.send_async(PmMsg::Register).await?;
        let task_reg_info = reg_recv.recv_async().await?;

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
                    .send_async(PmMsg::PieceFailed(pt.pid))
                    .await?;
                tracing::info!("failure while requesting piece '{}'", pt.pid);
            }
        }

        self.pm_sender
            .send_async(PmMsg::Deregister(self.id))
            .await?;

        res
    }

    /// The main peer session loop
    async fn process(&mut self) -> Result<(), PeerErr> {
        const TIMEOUT: u64 = 30;

        let mut msg_stream = self.setup_connection().await?;

        let mut first_message = true;
        loop {
            self.pick_piece().await?;
            self.queue_requests(&mut msg_stream).await?;
            self.send_interested(&mut msg_stream).await?;

            // Abort if we haven't received a message in <TIMEOUT> seconds
            tokio::select! {
                Ok(_) = self.notify_recv.changed() => {
                    if let TorrentState::Complete = *self.notify_recv.borrow() {
                        tracing::debug!("Exiting - task '{}' received torrent completion notification", self.id);
                        return Ok(());
                    }
                }
                Ok(msg) = tokio::time::timeout(Duration::from_secs(TIMEOUT), async {
                    msg_stream.next().await
                }) => {
                    let m = msg.ok_or(PeerErr::Terminated)??;
                    self.on_msg_received(m, first_message).await?;
                    first_message = false;
                }
            }
        }
    }

    async fn on_msg_received(&mut self, msg: Message, first_message: bool) -> Result<(), PeerErr> {
        match msg {
            Message::KeepAlive => tracing::debug!("Peer '{}' keep-alive", self.id),
            Message::Choke => self.on_choke_msg().await?,
            Message::Unchoke => self.on_unchoke_msg(),
            Message::Have(piece_id) => self.on_have_msg(piece_id).await?,
            Message::Bitfield(bitfield) => self.on_bitfield_msg(first_message, bitfield).await?,
            Message::Piece {
                index,
                begin,
                block,
            } => self.on_block_receive_msg(index, begin, block).await?,
            m => tracing::warn!("Unimplmeneted message received: {:?}", m),
        }

        Ok(())
    }

    fn on_unchoke_msg(&mut self) {
        tracing::debug!("Peer '{}' unchoked", self.id);
        self.am_choked = false;
    }

    async fn on_bitfield_msg(
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

    async fn on_have_msg(&mut self, piece_id: u32) -> Result<(), PeerErr> {
        tracing::debug!("Peer '{}' has new piece '{}'", self.id, piece_id);

        self.pm_sender
            .send_async(PmMsg::Have(self.id, piece_id))
            .await?;

        Ok(())
    }

    async fn on_choke_msg(&mut self) -> Result<(), PeerErr> {
        tracing::debug!("Peer '{}' choked", self.id);

        self.am_choked = true;

        if let Some(pt) = &self.piece_tracker {
            self.pm_sender
                .send_async(PmMsg::PieceFailed(pt.pid))
                .await?;
        }

        self.piece_tracker = None;
        Ok(())
    }

    async fn on_block_receive_msg(
        &mut self,
        index: u32,
        begin: u32,
        block: BytesMut,
    ) -> Result<(), PeerErr> {
        tracing::trace!("Task '{}' received a block", self.id);

        if let Some(pt) = &mut self.piece_tracker {
            if index != pt.pid {
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
        // UNWRAP: safe because we checked in self.piece tracker is Some
        let pt = std::mem::take(&mut self.piece_tracker).unwrap();
        let pid = pt.pid;
        let valid = pt.validate_piece(&self.metainfo)?;

        if let Some(vp) = valid {
            tracing::debug!("Piece '{}' finished", pid);

            self.pm_sender.send_async(PmMsg::PieceFinished(vp)).await?;
        } else {
            tracing::info!("Piece '{}' bad hash", pid);

            self.pm_sender.send_async(PmMsg::PieceFailed(pid)).await?;
        }

        Ok(())
    }

    /// If we are currently downloading a piece, ask the peer for some new
    /// blocks if needed
    async fn queue_requests(&mut self, msg_stream: &mut MsgStream) -> Result<(), PeerErr> {
        if let Some(pt) = &mut self.piece_tracker {
            let piece_id = pt.pid;
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

    /// Set up a TCP connection, exchange and validate handshakes
    async fn setup_connection(&mut self) -> Result<MsgStream, PeerErr> {
        let mut stream = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            TcpStream::connect(self.socket_addr),
        )
        .await
        .map_err(|_| PeerErr::Timeout)??;

        tracing::debug!(
            "Successfull connection: '{:?}'. Sending a handshake.",
            self.socket_addr
        );

        stream.write_all(&self.handshake.inner).await?;

        let mut peer_handshake = Handshake::new_empty();
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            stream.read_exact(&mut peer_handshake.inner).await
        })
        .await
        .map_err(|_| PeerErr::Timeout)??;

        let _peer_id = peer_handshake.validate(&peer_handshake)?;
        let msg_stream = MsgStream::new(stream, MessageCodec);

        tracing::debug!("Handshake with '{:?}' complete", self.socket_addr);

        Ok(msg_stream)
    }
}

#[derive(Error, Debug)]
pub enum PeerErr {
    #[error("TCP IO error: '{0}'")]
    FailedConnection(#[from] tokio::io::Error),
    #[error("Peer timed out")]
    Timeout,

    #[error("The session was terminated by the peer")]
    Terminated,
    #[error("Received an invalid handshake")]
    InvalidHandshake,
    #[error("Received a block related to a different piece")]
    InvalidPieceReceived,
    #[error("Received a block of invalid size")]
    InvalidBlockSize,
    #[error("'Bitfield' message wasn't the first message")]
    BitfieldNotFirst,

    #[error("Received an invalid piece ID from the PieceManager")]
    InvalidPiecePick,

    #[error("Message decoding error: '{0}'")]
    InvalidMessage(#[from] MessageDecodeErr),
    #[error("Message encoding error: '{0}'")]
    MessageEncodeErr(#[from] MessageEncodeErr),

    #[error("Channel error while sending messages to Piece Manager")]
    ChannelPmErr(#[from] flume::SendError<PmMsg>),
    #[error("Channel error while receiving messages from Piece Manager")]
    ChannelRecvErr(#[from] flume::RecvError),
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
