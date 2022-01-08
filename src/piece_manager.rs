use bytes::BytesMut;
use flume::{Receiver, Sender};
use std::collections::{hash_map::Entry, HashMap};
use tokio::sync::watch::{self, Receiver as WReceiver, Sender as WSender};

use crate::io::PieceIoMsg;

pub type TaskId = u32;
pub type PieceId = u32;

pub struct PieceManager {
    /// ID for the next registered peer
    next_id: TaskId,
    peers: HashMap<TaskId, Sender<PieceMsg>>,
    /// Map of which peers have which pieces
    peers_pieces: HashMap<TaskId, BitField>,
    piece_queue: PieceQueue,
    /// Receives all kinds of messages
    pm_recv: Receiver<PmMsg>,
    /// Sends TaskIDd and PieceId rx for a new task
    register_sender: Sender<TaskRegInfo>,
    /// Sends downloaded and verified pieces to the IO task
    io_sender: Sender<PieceIoMsg>,
    /// For notifying all peer tasks that the torrent is complete
    /// TODO: Maybe there's a better way of doing this, but tokio::sync::Notify
    /// didn't seem useful because notify_waiters() doesn't notify waiters that
    /// will register after the call to notified()
    notify_sender: WSender<TorrentState>,

    /// Total amount of pieces in the torrent
    num_pieces: u32,
    /// Total amount of pieces minus successfully downloaded and verified pieces
    missing_pieces: u32,
}

impl PieceManager {
    pub fn new(
        num_pieces: u32,
        io_sender: Sender<PieceIoMsg>,
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
            peers: HashMap::new(),
            peers_pieces: HashMap::new(),
            piece_queue: PieceQueue::new(num_pieces),
            pm_recv,
            register_sender,
            io_sender,
            notify_sender,
            num_pieces,
            missing_pieces: num_pieces,
        };

        (slf, pm_sender, register_recv, notify_recv)
    }

    pub fn add_peer(&mut self) -> TaskRegInfo {
        let task_id = self.next_id;
        self.next_id += 1;

        let (piece_sender, piece_recv) = flume::bounded(1);
        self.peers.insert(task_id, piece_sender);

        TaskRegInfo::new(task_id, piece_recv)
    }

    pub async fn piece_manager(mut self) {
        loop {
            if self.missing_pieces == 0 && self.peers.is_empty() {
                tracing::info!("Exiting - all pieces downloaded, all peer tasks deregistered");
                return;
            }

            let msg = self.pm_recv.recv_async().await.expect("Shouldn't panic ?");
            match msg {
                PmMsg::Have(task_id, piece_id) => self.on_have(task_id, piece_id),
                PmMsg::Bitfield(task_id, bitfield) => self.on_bitfield(task_id, bitfield),
                PmMsg::Pick(task_id) => self.on_pick(task_id).await,
                PmMsg::PieceRestart(pid) => self.piece_queue.requeue_piece(pid),
                PmMsg::PieceFinished(piece_msg) => self.on_piece_finished(piece_msg).await,
                PmMsg::Register => self.on_register().await,
                PmMsg::Deregister(task_id) => self.on_deregister(task_id),
            }
        }
    }

    fn on_have(&mut self, task_id: u32, piece_id: u32) {
        let bitfield = if let Entry::Vacant(e) = self.peers_pieces.entry(task_id) {
            e.insert(BitField::new_empty(self.num_pieces));
            self.peers_pieces.get_mut(&task_id).unwrap()
        } else {
            self.peers_pieces.get_mut(&task_id).unwrap()
        };

        bitfield.set_piece(piece_id);
    }

    fn on_bitfield(&mut self, task_id: u32, bitfield: BytesMut) {
        let bitfield = BitField::new(bitfield);
        self.peers_pieces.insert(task_id, bitfield);
    }

    async fn on_pick(&mut self, task_id: TaskId) {
        tracing::debug!("Task {} is requesting a piece pick", task_id);

        let piece = self
            .peers_pieces
            .get(&task_id)
            .and_then(|ap| self.piece_queue.next(ap));

        let msg = match piece {
            Some(pid) => PieceMsg::Piece(pid),
            None => PieceMsg::NoneAvailable,
        };

        let peer_sender = self.peers.get(&task_id).expect("Shouldn't happen");
        peer_sender.send_async(msg).await.expect("Shouldn't happen");
    }

    async fn on_piece_finished(&mut self, piece_msg: PieceIoMsg) {
        self.missing_pieces -= 1;
        if self.missing_pieces == 0 {
            self.notify_sender
                .send(TorrentState::Complete)
                .expect("Shouldn't happen");
        }

        self.io_sender
            .send_async(piece_msg)
            .await
            .expect("Shouldn't happen");
    }

    async fn on_register(&mut self) {
        let task_info = self.add_peer();
        self.register_sender
            .send_async(task_info)
            .await
            .expect("Shouldn't panic ?");
    }

    fn on_deregister(&mut self, task_id: u32) {
        self.peers.remove(&task_id);
        self.peers_pieces.remove(&task_id);

        tracing::debug!(
            "Deregistering peer task '{}', tasks left: '{}'",
            task_id,
            self.peers.len()
        );
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
    PieceRestart(PieceId),
    /// Peer task successfully downloaded and validated a piece
    PieceFinished(PieceIoMsg),

    /// Peer task wants to be registered
    Register,
    /// Peer task wants to be deregistered
    Deregister(TaskId),
}

/// Simple mechanism for sequential order piece picking
struct PieceQueue {
    blocks: Vec<(PieceId, PieceId)>,
}

impl PieceQueue {
    fn new(num_pieces: u32) -> Self {
        // Num pieces starts from 0
        let mut blocks = vec![(0, num_pieces - 1)];
        blocks.reserve(100);

        Self { blocks }
    }

    fn next(&mut self, available_pieces: &BitField) -> Option<PieceId> {
        // Find available piece
        let (block_index, piece) = self.blocks.iter().enumerate().find_map(|(i, block)| {
            for p in block.0..=block.1 {
                if available_pieces.has_piece(p) {
                    return Some((i, p));
                }
            }

            None
        })?;

        // Resize or split the block if needed
        let (mut start, mut end) = self.blocks[block_index];
        if piece == start {
            if start == end {
                self.blocks.remove(block_index);
            } else {
                start += 1;
                self.blocks[block_index] = (start, end);
            }
        } else if piece == end {
            // start == end case is already handled
            end -= 1;
            self.blocks[block_index] = (start, end);
        } else {
            self.blocks[block_index] = (piece + 1, end);
            self.blocks.insert(block_index, (start, piece - 1));
        }

        Some(piece)
    }

    /// If a piece wasn't downloaded successfully, add it to the queue again
    fn requeue_piece(&mut self, id: PieceId) {
        // TODO: could do something smarter here, but the fail-rate is pretty low
        self.blocks.push((id, id));
    }
}

struct BitField {
    inner: BytesMut,
}

impl BitField {
    /// Assumes the bitfield has correct length already
    fn new(bytes: BytesMut) -> Self {
        Self { inner: bytes }
    }

    fn new_empty(num_pieces: u32) -> Self {
        let len = (num_pieces as f32 / 8f32).ceil() as usize;
        let inner = vec![0; len];
        let inner = BytesMut::from(&inner[..]);
        Self { inner }
    }

    /// Is this piece available ?
    pub fn has_piece(&self, id: PieceId) -> bool {
        let byte = self
            .inner
            .get((id / 8) as usize)
            // This shouldn't happen
            .expect("Piece index out of bounds");

        let mask = Self::byte_mask(id);
        (*byte & mask) != 0
    }

    /// Sets the piece of 'id' to available
    pub fn set_piece(&mut self, id: PieceId) {
        let byte = self
            .inner
            .get_mut((id / 8) as usize)
            // This shouldn't happen
            .expect("Piece index out of bounds");

        let mask = Self::byte_mask(id);
        *byte |= mask;
    }

    /// Returns the bitmask corresponding to the piece id
    fn byte_mask(id: PieceId) -> u8 {
        128u8 >> (id % 8)
    }
}

#[cfg(test)]
mod test_super {
    use super::*;

    #[test]
    fn test_bitfield() {
        let bf = vec![0b0000_1000, 0b1001_0001];
        let bf = BytesMut::from(&bf[..]);
        let mut bf = BitField::new(bf);

        assert!(!bf.has_piece(0));
        assert!(!bf.has_piece(3));
        assert!(!bf.has_piece(7));
        assert!(bf.has_piece(4));

        bf.set_piece(0);
        bf.set_piece(3);
        bf.set_piece(7);

        assert!(bf.has_piece(0));
        assert!(bf.has_piece(3));
        assert!(bf.has_piece(7));
        assert!(bf.has_piece(4));

        assert!(bf.has_piece(8));
        assert!(bf.has_piece(11));
        assert!(bf.has_piece(15));

        bf.set_piece(9);
        bf.set_piece(13);

        assert!(bf.has_piece(8));
        assert!(bf.has_piece(9));
        assert!(bf.has_piece(11));
        assert!(bf.has_piece(13));
        assert!(bf.has_piece(15));
    }

    #[test]
    fn test_piece_queue() {
        let mut ap = BitField::new_empty(33);

        let mut pq = PieceQueue::new(33);
        assert_eq!(pq.blocks, &[(0, 32)]);

        ap.set_piece(16);
        ap.set_piece(4);
        assert_eq!(pq.next(&ap).unwrap(), 4);
        assert_eq!(pq.next(&ap).unwrap(), 16);

        assert_eq!(pq.blocks, &[(0, 3), (5, 15), (17, 32)]);
    }
}
