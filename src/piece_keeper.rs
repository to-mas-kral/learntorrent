use std::collections::{hash_map::Entry, HashMap};

use bytes::BytesMut;
use flume::{Receiver, Sender};
use rand::Rng;
use tokio::sync::{mpsc, oneshot::Sender as OSender, watch};

use crate::{io::IoMsg, p2p::ValidatedPiece, AppState};

use self::{bitfield::BitField, piece_list::PieceList};

mod bitfield;
mod piece_list;

pub type TaskId = u32;
pub type PieceId = u32;

pub struct PieceKeeper {
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
    /// Sends downloaded and verified pieces to the IO task
    io_sender: mpsc::Sender<IoMsg>,
    /// For notifying all peer tasks that the torrent is complete
    notify_sender: watch::Sender<TorrentState>,
    /// Passed to peer tasks through TaskRegMsg
    notify_recv: watch::Receiver<TorrentState>,
    /// AppStates notification
    appstate_recv: watch::Receiver<AppState>,
    should_exit: bool,
}

impl PieceKeeper {
    pub fn new(
        appstate_recv: watch::Receiver<AppState>,
        pm_recv: Receiver<PmMsg>,
        io_sender: mpsc::Sender<IoMsg>,
        num_pieces: u32,
    ) -> Self {
        let (notify_sender, notify_recv) = watch::channel(TorrentState::InProgress);

        Self {
            peers_pieces: HashMap::new(),
            piece_queue: piece_list::PieceList::new_full(num_pieces),
            finished_pieces: piece_list::PieceList::new_empty(),
            pending_pieces: Vec::with_capacity(32),
            piece_count: num_pieces,
            missing_pieces: num_pieces,
            peers: HashMap::new(),
            pm_recv,
            io_sender,
            notify_sender,
            notify_recv,
            appstate_recv,
            should_exit: false,
        }
    }

    pub async fn start(mut self) {
        loop {
            // Wait for the peer tasks to deregister
            if self.should_exit && self.peers.is_empty() {
                // Wait for the I/O task to save pending pieces to disk
                self.io_sender.send(IoMsg::Exit).await.expect(
                    "Internal error: Piece Keeper couldn't send exit message to the I/O task. \
                    It is possible that the downloaded file is corrupted !",
                );

                let _ = self.io_sender.closed().await;

                tracing::info!(
                    "Exiting - all pending pieces sent to I/O, all peer tasks deregistered"
                );
                return;
            }

            tokio::select! {
                // If all pm_senders are dropped, then we should either receive an appstate
                // exit notification or some other task panicked, in which case we should
                // panic as well
                Ok(msg) = self.pm_recv.recv_async() => {
                    match msg {
                        PmMsg::Have(task_id, piece_id) => self.on_have_msg(task_id, piece_id),
                        PmMsg::Bitfield(task_id, bitfield) => self.on_bitfield_msg(task_id, bitfield),
                        PmMsg::Pick(task_id) => self.on_pick_msg(task_id).await,
                        PmMsg::PieceFailed(pid) => self.on_piece_failed(pid),
                        PmMsg::PieceFinished(piece_msg) => self.on_piece_finished_msg(piece_msg).await,
                        PmMsg::Register(task_id, reg_sender) => {
                            self.on_register_msg(task_id, reg_sender).await
                        }
                        PmMsg::Deregister(task_id) => self.on_deregister_msg(task_id),
                    }
                }
                appstate_notif = self.appstate_recv.changed() => {
                    appstate_notif
                        .expect("Internal error: Piece Keeper couldn't receive \
                            appstate notificatoin from the Main Task");

                    if let AppState::Exit = *self.appstate_recv.borrow() {
                        tracing::info!("Piece Keeper received the app exit message");

                        self.should_exit = true;
                    }
                }
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

    async fn on_pick_msg(&mut self, task_id: TaskId) {
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

        let peer_sender = self
            .peers
            .get(&task_id)
            .expect("Internal error: Piece Keeper couldn't find a task with the given ID'");
        peer_sender
            .send_async(msg)
            .await
            .expect("Internal error: Piece Keeper couldn't send a PieceMsg to a peer task");
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
        if self.finished_pieces.contains(pid).is_none() && !self.pending_pieces.contains(&pid) {
            self.piece_queue.insert(pid)
        }
    }

    async fn on_piece_finished_msg(&mut self, piece_msg: ValidatedPiece) {
        let index = self
            .pending_pieces
            .iter()
            .position(|&pid| pid == piece_msg.pid);

        // Multiple peer tasks could've been given the same piece in endgame mode,
        // don't proceed if the piece has already been processed
        match index {
            Some(i) => self.pending_pieces.remove(i),
            None => return,
        };

        self.finished_pieces.insert(piece_msg.pid);

        self.missing_pieces -= 1;
        // TODO(UI): move this somewhere else
        let ratio = if self.missing_pieces == 0 {
            1f64
        } else {
            (self.piece_count - self.missing_pieces) as f64 / self.piece_count as f64
        };

        tracing::info!("Progress: {:.1} %", ratio * 100f64);

        if self.missing_pieces == 0 {
            tracing::info!("All pieces downloaded");
            tracing::debug!("Sending torrent completion notification");

            // UNWRAP: this struct has a copy of the receiver
            self.notify_sender.send(TorrentState::Complete).unwrap();
        }

        self.io_sender
            .send(IoMsg::Piece(piece_msg))
            .await
            .expect("Internal error: Piece Keeper couldn't send a message to the I/O task");
    }

    async fn on_register_msg(&mut self, task_id: TaskId, reg_sender: OSender<TaskRegMsg>) {
        tracing::debug!("Registering peer task '{}'", task_id);

        let (piece_sender, piece_recv) = flume::bounded(1);
        self.peers.insert(task_id, piece_sender);

        let task_info = TaskRegMsg::new(piece_recv, self.notify_recv.clone());
        reg_sender
            .send(task_info)
            .expect("Internal error: Piece Keeper couldn't send a TaskRegMsg to a peer task");
    }

    fn on_deregister_msg(&mut self, task_id: u32) {
        tracing::debug!("Deregistering peer task '{}'", task_id);

        self.peers.remove(&task_id);
        self.peers_pieces.remove(&task_id);
    }
}

#[derive(Debug)]
pub enum TorrentState {
    /// All pieces have been downloaded
    Complete,
    /// Torrent download in-progress
    InProgress,
}

#[derive(Debug)]
pub struct TaskRegMsg {
    pub piece_recv: Receiver<PieceMsg>,
    pub torrentstate_recv: watch::Receiver<TorrentState>,
}

impl TaskRegMsg {
    pub fn new(
        piece_recv: Receiver<PieceMsg>,
        torrentstate_recv: watch::Receiver<TorrentState>,
    ) -> Self {
        Self {
            piece_recv,
            torrentstate_recv,
        }
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
    Register(TaskId, OSender<TaskRegMsg>),
    /// Peer task wants to be deregistered
    Deregister(TaskId),
}
