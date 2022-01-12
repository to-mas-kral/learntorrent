use std::collections::{hash_map::Entry, HashMap};

use bytes::BytesMut;
use flume::{Receiver, Sender};
use rand::Rng;
use thiserror::Error;
use tokio::sync::watch::{self, Receiver as WReceiver, Sender as WSender};

use crate::p2p::ValidatedPiece;

use self::{bitfield::BitField, piece_list::PieceList};

mod bitfield;
mod piece_list;

pub type TaskId = u32;
pub type PieceId = u32;

pub struct PieceManager {
    /// ID for the next registered peer
    next_id: TaskId,
    /// Map of which peers have which pieces
    peers_pieces: HashMap<TaskId, BitField>,
    /// Pieces that have not been queued for downloaded, or have been restarted
    piece_queue: PieceList,
    /// Pieces that have been downloaded and verified
    finished_pieces: PieceList,
    /// Pieces which are currently being downloaded
    pending_pieces: Vec<PieceId>,
    /// Total amount of pieces in the torrent
    piece_count: u32,
    /// Total amount of pieces minus successfully downloaded and verified pieces
    missing_pieces: u32,

    /// Channels for communicating with individual peer tasks
    peers: HashMap<TaskId, Sender<PieceMsg>>,
    /// Receives all kinds of messages
    pm_recv: Receiver<PmMsg>,
    /// Sends TaskIDd and PieceId rx of a new task
    register_sender: Sender<TaskRegInfo>,
    /// Sends downloaded and verified pieces to the IO task
    io_sender: Sender<ValidatedPiece>,
    /// For notifying all peer tasks that the torrent is complete
    /// TODO: Maybe there's a better way of doing this, but tokio::sync::Notify
    /// didn't seem useful because notify_waiters() doesn't notify waiters that
    /// will register after the call to notified()
    notify_sender: WSender<TorrentState>,
}

impl PieceManager {
    pub fn new(
        num_pieces: u32,
        io_sender: Sender<ValidatedPiece>,
    ) -> (
        Self,
        Sender<PmMsg>,
        Receiver<TaskRegInfo>,
        WReceiver<TorrentState>,
    ) {
        let (pm_sender, pm_recv) = flume::unbounded();
        let (register_sender, register_recv) = flume::bounded(50);
        let (notify_sender, notify_recv) = watch::channel(TorrentState::Incomplete);

        let slf = Self {
            next_id: 0,
            peers_pieces: HashMap::new(),
            piece_queue: piece_list::PieceList::new_full(num_pieces),
            finished_pieces: piece_list::PieceList::new_empty(),
            pending_pieces: Vec::with_capacity(32),
            piece_count: num_pieces,
            missing_pieces: num_pieces,
            peers: HashMap::new(),
            pm_recv,
            register_sender,
            io_sender,
            notify_sender,
        };

        (slf, pm_sender, register_recv, notify_recv)
    }

    pub async fn piece_manager(mut self) -> Result<(), PmErr> {
        loop {
            if self.missing_pieces == 0 && self.peers.is_empty() {
                tracing::info!("Exiting - all pieces downloaded, all peer tasks deregistered");
                debug_assert!(self.pending_pieces.is_empty());
                debug_assert!(self.finished_pieces.all_finished(self.piece_count));
                return Ok(());
            }

            let msg = self.pm_recv.recv_async().await?;
            match msg {
                PmMsg::Have(task_id, piece_id) => self.on_have_msg(task_id, piece_id),
                PmMsg::Bitfield(task_id, bitfield) => self.on_bitfield_msg(task_id, bitfield),
                PmMsg::Pick(task_id) => self.on_pick_msg(task_id).await?,
                PmMsg::PieceFailed(pid) => self.on_piece_failed(pid),
                PmMsg::PieceFinished(piece_msg) => self.on_piece_finished_msg(piece_msg).await?,
                PmMsg::Register => self.on_register_msg().await?,
                PmMsg::Deregister(task_id) => self.on_deregister_msg(task_id),
            }
        }
    }

    fn on_have_msg(&mut self, task_id: u32, piece_id: u32) {
        if let Entry::Vacant(e) = self.peers_pieces.entry(task_id) {
            e.insert(BitField::new_empty(self.piece_count));
        }

        // UNWRAP: safe becuase we just inserted an empty bitfield
        let bitfield = self.peers_pieces.get_mut(&task_id).unwrap();

        bitfield.set_piece(piece_id);
    }

    fn on_bitfield_msg(&mut self, task_id: u32, bitfield: BytesMut) {
        let bitfield = BitField::new(bitfield);
        self.peers_pieces.insert(task_id, bitfield);
    }

    async fn on_pick_msg(&mut self, task_id: TaskId) -> Result<(), PmErr> {
        tracing::debug!("Task {} is requesting a piece pick", task_id);

        let msg = match self.peers_pieces.get(&task_id) {
            Some(ap) => match self.piece_queue.next(ap) {
                Some(pid) => {
                    self.pending_pieces.push(pid);
                    PieceMsg::Piece(pid)
                }
                None => self.endgame_pick(task_id),
            },
            None => PieceMsg::NoneAvailable,
        };

        let peer_sender = self.peers.get(&task_id).ok_or(PmErr::UnregisteredTask)?;
        peer_sender.send_async(msg).await?;

        Ok(())
    }

    fn endgame_pick(&mut self, task_id: u32) -> PieceMsg {
        let len = self.pending_pieces.len();

        if self.missing_pieces < 50 && len > 0 {
            let index = rand::thread_rng().gen_range(0..len);
            let pid = self.pending_pieces[index];

            tracing::debug!("Task '{}' given endgame piece '{}'", task_id, pid);

            PieceMsg::Piece(pid)
        } else {
            PieceMsg::NoneAvailable
        }
    }

    fn on_piece_failed(&mut self, pid: u32) {
        // A peer task could've encountered an error while downloading this piece,
        // but a different task could've already completed it in endgame mode,
        // or could be currently downloading the piece
        // TODO: could probably use binary_search() here
        if self.finished_pieces.contains(pid).is_none() && !self.pending_pieces.contains(&pid) {
            self.piece_queue.insert(pid)
        }
    }

    async fn on_piece_finished_msg(&mut self, piece_msg: ValidatedPiece) -> Result<(), PmErr> {
        let index = self
            .pending_pieces
            .iter()
            .position(|&pid| pid == piece_msg.pid);

        // Multiple peer tasks could've been given the same piece in endgame mode,
        // don't proceed if the piece has already been processed
        match index {
            Some(i) => self.pending_pieces.remove(i),
            None => return Ok(()),
        };

        self.finished_pieces.insert(piece_msg.pid);

        self.missing_pieces -= 1;
        // TODO: move this somewhere else
        let ratio = if self.missing_pieces == 0 {
            1f64
        } else {
            (self.piece_count - self.missing_pieces) as f64 / self.piece_count as f64
        };

        tracing::info!("Progress: {:.1} %", ratio * 100f64);

        if self.missing_pieces == 0 {
            tracing::debug!("Sending torrent completion notification");

            self.notify_sender.send(TorrentState::Complete)?;
        }

        self.io_sender.send_async(piece_msg).await?;

        Ok(())
    }

    async fn on_register_msg(&mut self) -> Result<(), PmErr> {
        let task_info = self.add_peer();
        self.register_sender.send_async(task_info).await?;

        Ok(())
    }

    fn add_peer(&mut self) -> TaskRegInfo {
        let task_id = self.next_id;
        self.next_id += 1;

        let (piece_sender, piece_recv) = flume::bounded(1);
        self.peers.insert(task_id, piece_sender);

        TaskRegInfo::new(task_id, piece_recv)
    }

    fn on_deregister_msg(&mut self, task_id: u32) {
        self.peers.remove(&task_id);
        self.peers_pieces.remove(&task_id);

        tracing::debug!("Deregistering peer task '{}'", task_id,);
    }
}

#[derive(Debug)]
pub enum TorrentState {
    /// All pieces have been downloaded
    Complete,
    /// Torrent download in-progress
    Incomplete,
}

pub struct TaskRegInfo {
    pub id: TaskId,
    pub piece_recv: Receiver<PieceMsg>,
}

impl TaskRegInfo {
    pub fn new(id: TaskId, piece_recv: Receiver<PieceMsg>) -> Self {
        Self { id, piece_recv }
    }
}

pub enum PieceMsg {
    /// Success
    Piece(PieceId),
    /// Either all of the pieces have already been successfully
    /// downloaded or the peer doesn't have any useful pieces
    NoneAvailable,
}

pub enum PmMsg {
    /// Peer task received a Have message
    Have(TaskId, PieceId),
    /// Peer task received a Bitfield message
    Bitfield(TaskId, BytesMut),
    /// Peer task requested a piece to be picked for it
    Pick(TaskId),
    /// Peer task couldn't validate or finish downloading the piece
    PieceFailed(PieceId),
    /// Peer task successfully downloaded and validated a piece
    PieceFinished(ValidatedPiece),

    /// Peer task wants to be registered
    Register,
    /// Peer task wants to be deregistered
    Deregister(TaskId),
}

#[derive(Error, Debug)]
pub enum PmErr {
    #[error("Channel error while notifying peer tasks")]
    TaskNotifyErr(#[from] watch::error::SendError<TorrentState>),
    #[error("Channel error while sending a register to a peer task")]
    RegisterSendErr(#[from] flume::SendError<TaskRegInfo>),
    #[error("Channel error while sending a message to the IO task")]
    IoTaskSendErr(#[from] flume::SendError<ValidatedPiece>),
    #[error("Channel error while sending a message to a peer task")]
    PeerTaskSendErr(#[from] flume::SendError<PieceMsg>),
    #[error("Channel error while receiving messages")]
    ChannelRecvErr(#[from] flume::RecvError),
    #[error("Error while trying to communicate with a task that is not currently registered")]
    UnregisteredTask,
}
