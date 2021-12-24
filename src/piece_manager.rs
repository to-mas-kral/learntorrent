use flume::{Receiver, Sender};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

pub type TaskId = u64;
pub type PieceId = u64;

pub type TaskInfo = (TaskId, Receiver<PieceId>);

pub struct PieceManager {
    next_id: TaskId,
    peers: HashMap<TaskId, Sender<PieceId>>,
    pieces: HashMap<PieceId, Vec<TaskId>>,
    /// Receives all kinds of message
    rx: Receiver<PmMessage>,
    /// Sends TaskIDd and PieceId rx for a new task
    register_tx: Sender<TaskInfo>,
}

impl PieceManager {
    pub fn new() -> (Self, Sender<PmMessage>, Receiver<TaskInfo>) {
        let (tx, rx) = flume::unbounded();
        let (register_tx, register_rx) = flume::bounded(1);

        let s = Self {
            next_id: 0,
            peers: HashMap::new(),
            pieces: HashMap::new(),
            rx,
            register_tx,
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
}

#[derive(Debug)]
pub enum PmMessage {
    /// Peer task received a Have message
    Have(TaskId, PieceId),
    /// Peer task received a Bitfield message
    HaveMultiple(TaskId, Vec<u8>),
    /// Peer task request a next PieceId for download
    Request(TaskId),
    /// Main task asks for a new peer task to be registered
    Register,
}

pub async fn piece_manager(mut pm: PieceManager) {
    loop {
        let msg = pm.rx.recv_async().await.expect("Shouldn't panic ?");
        match msg {
            PmMessage::Have(_, _) => todo!(),
            PmMessage::HaveMultiple(_, _) => todo!(),
            PmMessage::Request(task_id) => {
                tracing::info!("Task {} is requesting a piece", task_id)
            }
            PmMessage::Register => {
                let task_info = pm.add_peer();
                pm.register_tx
                    .send_async(task_info)
                    .await
                    .expect("Shouldn't panic ?");
            }
        }
    }
}
