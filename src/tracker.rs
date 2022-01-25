use std::{
    collections::{HashMap, VecDeque},
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
};

use flume::Sender as FSender;
use thiserror::Error;
use tokio::{
    sync::{
        mpsc::{self, Sender as MSender},
        watch::Receiver as WReceiver,
    },
    task::JoinHandle,
};

use crate::{
    bencoding::{
        bevalue::{BeStr, BeValue, ResponseParseError},
        BeDecodeErr,
    },
    metainfo::Metainfo,
    p2p::{Handshake, Peer, PeerErr},
    piece_manager::{PmMsg, TaskId, TorrentState},
};

/// The job of this object is to perdiocially resend the announce "request"
/// and manage an adequate number of peers (30 right now)
pub struct Tracker {
    /// Torrent metainfo
    metainfo: Arc<Metainfo>,
    /// Our ID, unique for every run
    client_id: Vec<u8>,

    /// ID for the next peer task
    next_id: TaskId,
    /// Map for keeping track of active peer tasks
    active_peers: HashMap<TaskId, JoinHandle<()>>,
    /// Queue of uncontacted available peers
    uncontacted_peers: VecDeque<SocketAddrV4>,
    /// Received a notification from PieceManager that all pieces have been downloaded,
    /// do not try to queue other peers after this is set
    torrent_complete: bool,

    /// PieceManagerMsg sender, passed to the tasks
    pm_sender: FSender<PmMsg>,
    /// TorrentState receiver, passed to the tasks
    notify_recv: WReceiver<TorrentState>,
}

impl Tracker {
    pub fn new(
        metainfo: Arc<Metainfo>,
        pm_sender: FSender<PmMsg>,
        notify_recv: WReceiver<TorrentState>,
    ) -> Self {
        let client_id = Self::gen_client_id();

        Self {
            metainfo,
            client_id,
            next_id: 0,
            active_peers: HashMap::new(),
            uncontacted_peers: VecDeque::new(),
            torrent_complete: false,
            pm_sender,
            notify_recv,
        }
    }

    pub async fn start(mut self) -> TrResult<()> {
        let req_url = self.build_announce_url(0, 0, 1000, ClientState::Started);

        let response = reqwest::get(req_url).await?.bytes().await?;
        let response = BeValue::from_bytes(&response)?;
        let response = TrackerResponse::from_bevalue(response)?;

        tracing::debug!("Parsed tracker response. Response: {:?}", &response);

        self.uncontacted_peers = VecDeque::from(response.peers);

        let handshake = Arc::new(Handshake::new(&self.client_id, &self.metainfo));

        let (completion_sender, mut completion_recv) = mpsc::channel::<TaskId>(50);

        self.queue_tasks(completion_sender.clone(), Arc::clone(&handshake))
            .await?;

        loop {
            if self.active_peers.is_empty() {
                return Ok(());
            }

            tokio::select! {
                Ok(_) = self.notify_recv.changed() => {
                    if let TorrentState::Complete = *self.notify_recv.borrow() {
                        self.torrent_complete = true;
                    }
                }
                Some(task_id) = completion_recv.recv() => {
                    tracing::debug!("Task '{}' completion message", task_id);

                    self.active_peers.remove(&task_id);

                    self.queue_tasks(completion_sender.clone(), Arc::clone(&handshake))
                    .await?;
                }
            }
        }
    }

    const MAX_ACTIVE_TASKS: usize = 20;

    async fn queue_tasks(
        &mut self,
        completion_sender: MSender<TaskId>,
        handshake: Arc<Handshake>,
    ) -> TrResult<()> {
        if self.torrent_complete {
            return Ok(());
        }

        let active_peers = self.active_peers.len();
        let to_queue = Self::MAX_ACTIVE_TASKS.saturating_sub(active_peers);

        for _ in 0..to_queue {
            if let Some(socket_addr) = self.uncontacted_peers.pop_front() {
                let task_id = self.next_id;
                self.next_id += 1;

                tracing::debug!("Created a new peer task with id of '{}'", task_id);

                let peer = Peer::create(
                    task_id,
                    socket_addr,
                    Arc::clone(&handshake),
                    Arc::clone(&self.metainfo),
                    self.pm_sender.clone(),
                    self.notify_recv.clone(),
                )
                .await?;

                let completion_sender = completion_sender.clone();

                let peer_task = tokio::spawn(async move {
                    if let Err(e) = peer.start().await {
                        tracing::debug!("Peer task error: '{:?}'", e);
                    }

                    completion_sender.clone().send(task_id).await.unwrap();
                });

                self.active_peers.insert(task_id, peer_task);
            }
        }

        Ok(())
    }

    const DEFAULT_PORT: u16 = 6881;

    fn build_announce_url(
        &mut self,
        uploaded: u64,
        downloaded: u64,
        left: u64,
        event: ClientState,
    ) -> String {
        return format!(
            "{announce}?info_hash={info_hash}&peer_id={peer_id}&port={port}&uploaded={uploaded}&downloaded={downloaded}&left={left}&event={event}&compact=1",
            announce = self.metainfo.announce,
            info_hash = urlencoding::encode_binary(&self.metainfo.info_hash),
            peer_id = urlencoding::encode_binary(&self.client_id),
            port = Self::DEFAULT_PORT,
            uploaded = uploaded,
            downloaded = downloaded,
            left = left,
            event = event.to_str(),
        );
    }

    fn gen_client_id() -> Vec<u8> {
        let mut id = Vec::with_capacity(20);
        id.extend_from_slice("-LO0001-".as_bytes());

        for _ in 0..12 {
            id.push(rand::random::<u8>());
        }

        id
    }
}

pub struct TrackerResponse {
    /// Interval in seconds that the client should wait between sending regular requests to the tracker
    interval: u64,
    /// number of peers with the entire file
    seeds: Option<u64>,
    /// number of non-seeder peers
    leeches: Option<u64>,
    /// IP addresses and ports of peers (currently only the binary model is supported)
    pub peers: Vec<SocketAddrV4>,
    /// A string that the client should send back on its next announcements.
    /// If absent and a previous announce sent a tracker id, do not discard the old value; keep using it.
    tracker_id: Option<BeStr>,
}

impl TrackerResponse {
    pub fn from_bevalue(mut be: BeValue) -> TrResult<Self> {
        let resp = be.get_dict()?;

        let interval = resp.expect("interval")?.get_uint()?;

        let seeds = resp.try_get("complete", BeValue::get_uint)?;
        let leeches = resp.try_get("incomplete", BeValue::get_uint)?;
        let tracker_id = resp.try_get("tracker id", BeValue::get_str)?;

        let peers = resp.expect("peers")?.get_str()?;
        if peers.len() % 6 != 0 {
            return Err(TrErr::InvalidPeersLen(peers.len()));
        }

        let peers = peers
            .chunks(6)
            .map(|c| {
                SocketAddrV4::new(
                    Ipv4Addr::new(c[0], c[1], c[2], c[3]),
                    u16::from_be_bytes([c[4], c[5]]),
                )
            })
            .collect();

        Ok(TrackerResponse {
            interval,
            seeds,
            leeches,
            peers,
            tracker_id,
        })
    }
}

impl std::fmt::Debug for TrackerResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrackerResponse")
            .field("interval", &self.interval)
            .field("seeds", &self.seeds)
            .field("leeches", &self.leeches)
            .field("peers", &format_args!("<peer IP addresses>"))
            .field("tracker_id", &self.tracker_id)
            .finish()
    }
}

type TrResult<T> = Result<T, TrErr>;

#[derive(Error, Debug)]
pub enum TrErr {
    #[error("{0}")]
    BeError(#[from] ResponseParseError),
    #[error("Bencode decoding error while parsing the tracker response: '{0}'")]
    BeDecodeError(#[from] BeDecodeErr),
    #[error("Peers string length '{0}' should be a multiple of 6")]
    InvalidPeersLen(usize),

    #[error("Failed HTTP request to the tracker: '{0}'")]
    HttpErr(#[from] reqwest::Error),
    #[error("Peer error: '{0}'")]
    PeerErr(#[from] PeerErr),

    #[error("Tokio join error: '{0}'")]
    JoinError(#[from] tokio::task::JoinError),
}

#[derive(Clone, Copy)]
pub enum ClientState {
    Started,
    //Stopped,
    //Paused,
}

impl ClientState {
    pub fn to_str(self) -> &'static str {
        match self {
            ClientState::Started => "started",
            //ClientState::Stopped => "stopped",
            //ClientState::Paused => "paused",
        }
    }
}

#[cfg(test)]
mod test_super {
    #[test]
    fn test_urlencoding() {
        assert_eq!(
            urlencoding::encode_binary(&[
                0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf1, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd,
                0xef, 0x12, 0x34, 0x56, 0x78, 0x9a
            ]),
            "%124Vx%9A%BC%DE%F1%23Eg%89%AB%CD%EF%124Vx%9A"
        );
    }
}
