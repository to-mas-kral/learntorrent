use std::{net::SocketAddrV4, sync::Arc, time::Duration};

use bytes::BytesMut;
use flume::{Receiver, Sender};
use futures::sink::SinkExt;
use futures_util::StreamExt;
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{oneshot, watch},
};
use tokio_util::codec::Framed;

use crate::{
    metainfo::Metainfo,
    p2p::piece_tracker::{CompletedBlockRequest, PieceTracker},
    piece_keeper::{PieceMsg, PmMsg, TaskId, TaskRegMsg, TorrentState},
    AppState,
};

use self::message::{Message, MessageCodec, MessageDecodeErr, MessageEncodeErr};

mod message;
mod piece_tracker;

pub use piece_tracker::{ValidatedPiece, BLOCK_LEN};

type MsgStream = Framed<TcpStream, MessageCodec>;

pub struct Peer {
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
    torrentstate_recv: watch::Receiver<TorrentState>,
    /// Receiver for AppState notification
    appstate_recv: watch::Receiver<AppState>,

    /// Torrent metainfo
    metainfo: Arc<Metainfo>,

    // Is this task choked by the peer ?
    am_choked: bool,
    // Has this task already expressed interest in receiving pieces ?
    am_interested: bool,

    /// Holds information about the progress of the currently downloaded piece
    piece_tracker: Option<PieceTracker>,
}

impl Peer {
    pub fn new(
        id: TaskId,
        socket_addr: SocketAddrV4,
        handshake: Arc<Handshake>,
        pm_sender: Sender<PmMsg>,
        piece_recv: Receiver<PieceMsg>,
        torrentstate_recv: watch::Receiver<TorrentState>,
        appstate_recv: watch::Receiver<AppState>,
        metainfo: Arc<Metainfo>,
    ) -> Self {
        Self {
            id,
            socket_addr,
            handshake,
            pm_sender,
            piece_recv,
            torrentstate_recv,
            appstate_recv,
            metainfo,
            am_choked: true,
            am_interested: false,
            piece_tracker: None,
        }
    }

    pub async fn create(
        id: TaskId,
        socket_addr: SocketAddrV4,
        handshake: Arc<Handshake>,
        metainfo: Arc<Metainfo>,
        pm_sender: Sender<PmMsg>,
        appstate_recv: watch::Receiver<AppState>,
    ) -> Peer {
        // TODO: this will have to be a broadcast channel for the 'Have' messages...
        let (reg_sender, reg_recv) = oneshot::channel::<TaskRegMsg>();

        pm_sender
            .send_async(PmMsg::Register(id, reg_sender))
            .await
            .expect(
            "Internal error: a peer task couldn't send a 'register' message to the Piece Keeper",
        );

        let task_reg_msg = reg_recv.await.expect(
            "Internal error: a peer task couldn't receive a 'TaskRegMsg' from the Piece Keeper",
        );

        Peer::new(
            id,
            socket_addr,
            handshake,
            pm_sender,
            task_reg_msg.piece_recv,
            task_reg_msg.torrentstate_recv,
            appstate_recv,
            metainfo,
        )
    }

    pub async fn start(mut self) -> PeerResult<()> {
        let res = self.process().await;

        if res.is_err() {
            if let Some(pt) = &self.piece_tracker {
                self.pm_sender
                    .send_async(PmMsg::PieceFailed(pt.pid))
                    .await
                    .expect(
                        "Internal error: a peer task couldn't send a 'piece retry' message to the Piece Keeper",
                    );
                tracing::info!("Failure while requesting piece '{}'", pt.pid);
            }
        }

        self.pm_sender
            .send_async(PmMsg::Deregister(self.id))
            .await
            .expect(
            "Internal error: a peer task couldn't send a 'deregister' message to the Piece Keeper",
        );

        res
    }

    /// The main peer session loop
    async fn process(&mut self) -> PeerResult<()> {
        const TIMEOUT: u64 = 30;

        let mut msg_stream = self.setup_connection().await?;

        let mut first_message = true;
        loop {
            self.pick_piece().await?;
            self.queue_requests(&mut msg_stream).await?;
            self.send_interested(&mut msg_stream).await?;

            tokio::select! {
                Ok(_) = self.appstate_recv.changed() => {
                    if let AppState::Exit = *self.appstate_recv.borrow() {
                        tracing::debug!("Exiting - task '{}' received app exit notification", self.id);
                        return Ok(());
                    }
                }
                Ok(_) = self.torrentstate_recv.changed() => {
                    if let TorrentState::Complete = *self.torrentstate_recv.borrow() {
                        tracing::debug!("Exiting - task '{}' received torrent completion notification", self.id);
                        return Ok(());
                    }
                }
                msg = tokio::time::timeout(Duration::from_secs(TIMEOUT), msg_stream.next()) => {
                    let m = msg.map_err(|_| PeerErr::Timeout)?.ok_or(PeerErr::Terminated)??;
                    self.on_msg_received(m, first_message).await?;
                    first_message = false;
                }
            }
        }
    }

    async fn on_msg_received(&mut self, msg: Message, first_message: bool) -> PeerResult<()> {
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

    async fn on_bitfield_msg(&mut self, first_message: bool, bitfield: BytesMut) -> PeerResult<()> {
        if first_message {
            tracing::debug!("Peer '{}' has a bitfield", self.id);

            self.verify_bitfield(&bitfield)?;

            self.pm_sender
                .send_async(PmMsg::Bitfield(self.id, bitfield))
                .await
                .expect("Internal error: a peer task couldn't send a 'bitfield' message to the Piece Keeper");
        } else {
            return Err(PeerErr::BitfieldNotFirst);
        }

        Ok(())
    }

    fn verify_bitfield(&self, bitfield: &[u8]) -> PeerResult<()> {
        let piece_count = self.metainfo.piece_count();

        // TODO: integer ceil_div when stabilized (and also in other places)
        let expected_len = (piece_count as f32 / 8f32).ceil() as usize;

        if bitfield.len() != expected_len {
            return Err(PeerErr::InvalidBitfieldMsg);
        }

        // Spare bits at the end should be set to zero
        let spare_bits = (expected_len * 8) as u32 - piece_count;
        let mask = match spare_bits {
            0 => 0b0000_0000u8,
            1 => 0b0000_0001,
            2 => 0b0000_0011,
            3 => 0b0000_0111,
            4 => 0b0000_1111,
            5 => 0b0001_1111,
            6 => 0b0011_1111,
            7 => 0b0111_1111,
            8 => 0b1111_1111,
            _ => unreachable!(),
        };

        // UNWRAP: can't panic since we checked the length
        if bitfield.last().unwrap() & mask != 0 {
            return Err(PeerErr::InvalidBitfieldMsg);
        }

        Ok(())
    }

    async fn on_have_msg(&mut self, piece_id: u32) -> PeerResult<()> {
        tracing::debug!("Peer '{}' has new piece '{}'", self.id, piece_id);

        self.pm_sender
            .send_async(PmMsg::Have(self.id, piece_id))
            .await
            .expect(
                "Internal error: a peer task couldn't send a 'have' message to the Piece Keeper",
            );

        Ok(())
    }

    async fn on_choke_msg(&mut self) -> PeerResult<()> {
        tracing::debug!("Peer '{}' choked", self.id);

        self.am_choked = true;

        if let Some(pt) = &self.piece_tracker {
            self.pm_sender
                .send_async(PmMsg::PieceFailed(pt.pid))
                .await
                .expect("Internal error: a peer task couldn't send a 'piece retry' message to the Piece Keeper");
        }

        self.piece_tracker = None;
        Ok(())
    }

    async fn on_block_receive_msg(
        &mut self,
        index: u32,
        begin: u32,
        block: BytesMut,
    ) -> PeerResult<()> {
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

            self.pm_sender.send_async(PmMsg::PieceFinished(vp)).await
                .expect("Internal error: a peer task couldn't send a 'piece finished' message to the Piece Keeper");
        } else {
            tracing::info!("Piece '{}' bad hash", pid);

            self.pm_sender.send_async(PmMsg::PieceFailed(pid)).await
                .expect("Internal error: a peer task couldn't send a 'piece retry' message to the Piece Keeper");
        }

        Ok(())
    }

    /// If we are currently downloading a piece, ask the peer for some new
    /// blocks if needed
    async fn queue_requests(&mut self, msg_stream: &mut MsgStream) -> PeerResult<()> {
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
    async fn pick_piece(&mut self) -> PeerResult<()> {
        if !self.am_choked && self.piece_tracker.is_none() {
            self.pm_sender
                .send_async(PmMsg::Pick(self.id))
                .await
                .expect("Internal error: a peer task couldn't send a 'piece pick' message to the Piece Keeper");
            let piece_msg = self.piece_recv.recv_async().await
                .expect("Internal error: a peer task couldn't receive a 'piece pick' message from the Piece Keeper");

            if let PieceMsg::Piece(pid) = piece_msg {
                tracing::debug!("Task '{}' picked piece '{}'", self.id, pid);

                let piece_size = self.metainfo.get_piece_size(pid);
                self.piece_tracker = Some(PieceTracker::new(pid, piece_size));
            }
        }

        Ok(())
    }

    /// If we are choked and not interested, send an Interested message to the peer
    async fn send_interested(&mut self, msg_stream: &mut MsgStream) -> PeerResult<()> {
        if !self.am_interested && self.am_choked {
            self.am_interested = true;
            msg_stream.send(Message::Interested).await?;
        }

        Ok(())
    }

    // TODO: make TCP connection and handshake cancellable
    /// Set up a TCP connection, exchange and validate handshakes
    async fn setup_connection(&mut self) -> PeerResult<MsgStream> {
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
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            stream.read_exact(&mut peer_handshake.inner),
        )
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
    #[error("TCP conenction error: '{0}'")]
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
    #[error("'Bitfield' message wasn invalid")]
    InvalidBitfieldMsg,

    #[error("Message decoding error: '{0}'")]
    InvalidMessage(#[from] MessageDecodeErr),
    #[error("Message encoding error: '{0}'")]
    MessageEncodeErr(#[from] MessageEncodeErr),
}

type PeerResult<T> = Result<T, PeerErr>;

pub struct Handshake {
    pub inner: Vec<u8>,
}

impl Handshake {
    const HANDSHAKE_LEN: usize = 68;

    pub fn new(client_id: &[u8; 20], metainfo: &Metainfo) -> Self {
        let mut handshake = vec![0; Self::HANDSHAKE_LEN];
        handshake[0] = 0x13;
        handshake[1..20].copy_from_slice("BitTorrent protocol".as_bytes());
        // Extensions, currently all 0
        handshake[20..28].fill(0);
        handshake[28..48].copy_from_slice(&*metainfo.info_hash);
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
