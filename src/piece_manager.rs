use bytes::BytesMut;
use flume::{Receiver, Sender};
use std::collections::{hash_map::Entry, HashMap};

pub type TaskId = u32;
pub type PieceId = u32;

pub type TaskInfo = (TaskId, Receiver<PieceId>);

pub struct PieceManager {
    next_id: TaskId,
    peers: HashMap<TaskId, Sender<PieceId>>,
    /// Map of which peers have which pieces
    peers_pieces: HashMap<TaskId, BitField>,
    piece_queue: PieceQueue,
    /// Receives all kinds of messages
    rx: Receiver<PmMessage>,
    /// Sends TaskIDd and PieceId rx for a new task
    register_tx: Sender<TaskInfo>,
    num_pieces: u32,
}

impl PieceManager {
    pub fn new(num_pieces: u32) -> (Self, Sender<PmMessage>, Receiver<TaskInfo>) {
        let (tx, rx) = flume::unbounded();
        let (register_tx, register_rx) = flume::bounded(1);

        let s = Self {
            next_id: 0,
            peers: HashMap::new(),
            peers_pieces: HashMap::new(),
            piece_queue: PieceQueue::new(num_pieces),
            rx,
            register_tx,
            num_pieces,
        };

        (s, tx, register_rx)
    }

    pub fn add_peer(&mut self) -> (TaskId, Receiver<PieceId>) {
        let task_id = self.next_id;
        self.next_id += 1;

        let (tx, rx) = flume::bounded(1);
        self.peers.insert(task_id, tx);

        (task_id, rx)
    }

    pub async fn piece_manager(mut self) {
        loop {
            let msg = self.rx.recv_async().await.expect("Shouldn't panic ?");
            match msg {
                PmMessage::Have(task_id, piece_id) => {
                    tracing::info!("Task {} has a new piece: {}", task_id, piece_id);

                    let bitfield = if let Entry::Vacant(e) = self.peers_pieces.entry(task_id) {
                        e.insert(BitField::new_empty(self.num_pieces));
                        self.peers_pieces.get_mut(&task_id).unwrap()
                    } else {
                        self.peers_pieces.get_mut(&task_id).unwrap()
                    };

                    bitfield.set_piece(piece_id);
                }
                PmMessage::HaveBitfield(task_id, bitfield) => {
                    tracing::info!("Task {} has a bitfield", task_id);

                    let bitfield = BitField::new(bitfield);
                    self.peers_pieces.insert(task_id, bitfield);
                }
                PmMessage::Pick(task_id) => {
                    tracing::info!("Task {} is requesting a piece pick", task_id);

                    // TODO: don't have any pieces from peer
                    let available_pieces = self.peers_pieces.get(&task_id).unwrap();
                    let piece = self.piece_queue.next(available_pieces);
                    tracing::warn!("{:?} - {:?}", piece, &self.piece_queue);

                    if let Some(piece) = piece {
                        let peer_sender = self.peers.get(&task_id).expect("Shouldn't happen");
                        peer_sender
                            .send_async(piece)
                            .await
                            .expect("Shouldn't happen");
                    }
                }
                PmMessage::Register => {
                    let task_info = self.add_peer();
                    self.register_tx
                        .send_async(task_info)
                        .await
                        .expect("Shouldn't panic ?");
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum PmMessage {
    /// Peer task received a Have message
    Have(TaskId, PieceId),
    /// Peer task received a Bitfield message
    HaveBitfield(TaskId, BytesMut),
    /// Peer task requested a piece to be picked for it
    Pick(TaskId),
    /// Main task asks for a new peer task to be registered
    Register,
}

#[derive(Debug)]
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
    fn requeue_piece(&mut self, id: PieceId) {}
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
