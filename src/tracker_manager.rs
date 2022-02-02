use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};

use thiserror::Error;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

use crate::{
    bencoding::{bevalue::ResponseParseError, BeDecodeErr},
    metainfo::Metainfo,
    p2p::{Handshake, Peer},
    piece_keeper::{PmMsg, TaskId},
    AppState,
};

use self::{
    http::HttpTracker,
    udp::{TrackerMsgDecodeErr, UdpTracker},
};

mod http;
mod udp;

/// The job of this object is to perdiocially resend the announce "request"
/// and manage an adequate number of peers (30 right now)
pub struct TrackerManager {
    /// Torrent metainfo
    metainfo: Arc<Metainfo>,
    /// Our ID, unique for every run
    client_id: Arc<[u8; 20]>,

    /// ID of the next peer task
    next_id: TaskId,
    /// Map for keeping track of active peer tasks
    active_peers: HashMap<TaskId, JoinHandle<()>>,
    /// Queue of uncontacted available peers
    queued_peers: VecDeque<SocketAddrV4>,
    // All received peers from all trackers, used for filtering duplicates
    all_peers: HashSet<Ipv4Addr>,

    // TODO: torrent_complete
    /// Received a notification from PieceManager that all pieces have been downloaded,
    /// do not queue other peers after this is set
    //torrent_complete: bool,

    /// PieceManagerMsg sender, passed to the tasks
    pm_sender: flume::Sender<PmMsg>,

    /// AppState notifications
    appstate_recv: watch::Receiver<AppState>,
    should_exit: bool,
}

impl TrackerManager {
    pub fn new(
        appstate_recv: watch::Receiver<AppState>,
        pm_sender: flume::Sender<PmMsg>,
        metainfo: Arc<Metainfo>,
    ) -> Self {
        let client_id = Self::gen_client_id();

        Self {
            metainfo,
            client_id,
            next_id: 0,
            active_peers: HashMap::new(),
            queued_peers: VecDeque::new(),
            all_peers: HashSet::new(),
            pm_sender,
            appstate_recv,
            should_exit: false,
        }
    }

    pub async fn start(mut self) -> TrResult<()> {
        let total_trackers = self.metainfo.trackers.len();
        let (tracker_sender, mut tracker_recv) = mpsc::channel::<Vec<SocketAddrV4>>(total_trackers);
        let tracker_tasks = self.spawn_trackers(&self.metainfo.trackers, tracker_sender);

        let (completion_sender, mut completion_recv) =
            mpsc::channel::<TaskId>(Self::MAX_ACTIVE_TASKS);

        let handshake = Arc::new(Handshake::new(&self.client_id, &self.metainfo));

        loop {
            if self.should_exit && self.active_peers.is_empty() {
                for t in tracker_tasks {
                    let res = t.await;

                    if let Err(e) = res {
                        tracing::warn!("{}", e);
                    }
                }

                tracing::info!("Exiting - all peer tasks completed, all tracker tasks completed");

                return Ok(());
            }

            self.queue_peer_tasks(completion_sender.clone(), handshake.clone())
                .await?;

            tokio::select! {
                Some(new_peers) = tracker_recv.recv() => {
                    /* let new_peers = new_peers
                        .expect("Internal error: Tracker Manager couldn't receive the \
                            completion notification from the tracker tasks"); */

                    for peer in new_peers {
                        if self.all_peers.insert(*peer.ip()) {
                            self.queued_peers.push_back(peer);
                        }
                    }

                    tracing::info!("Unique peers: '{}'", &self.all_peers.len());
                }
                // We have a copy of the completion sender, so recv() can't return None
                Some(task_id) = completion_recv.recv() => {
                    tracing::debug!("Task '{}' completion message", task_id);

                    self.active_peers.remove(&task_id);

                    self.queue_peer_tasks(completion_sender.clone(), Arc::clone(&handshake))
                    .await?;
                }
                appstate_notif = self.appstate_recv.changed() => {
                    appstate_notif
                        .expect("Internal error: Tracker Manager couldn't receive appstate \
                            notification from the Main Task");

                    if let AppState::Exit = *self.appstate_recv.borrow() {
                        tracing::info!("Tracker Manager received the app exit message");

                        self.should_exit = true;
                    }
                }
            }
        }
    }

    fn spawn_trackers(
        &self,
        trackers: &[String],
        tracker_sender: mpsc::Sender<Vec<SocketAddrV4>>,
    ) -> Vec<JoinHandle<()>> {
        let mut tasks = vec![];

        for url in trackers {
            let tracker_task = TrackerTask::new(
                url.clone(),
                self.metainfo.clone(),
                self.client_id.clone(),
                tracker_sender.clone(),
                self.appstate_recv.clone(),
            );

            let tracker_task = tokio::spawn(async move {
                let res = TrackerTask::start(tracker_task).await;

                if let Err(e) = res {
                    tracing::debug!("Tracker task error: '{}'", e);
                }
            });

            tasks.push(tracker_task);
        }

        tasks
    }

    const MAX_ACTIVE_TASKS: usize = 25;

    async fn queue_peer_tasks(
        &mut self,
        completion_sender: mpsc::Sender<TaskId>,
        handshake: Arc<Handshake>,
    ) -> TrResult<()> {
        if self.should_exit {
            return Ok(());
        }

        let active_peers = self.active_peers.len();
        let to_queue = Self::MAX_ACTIVE_TASKS.saturating_sub(active_peers);

        for _ in 0..to_queue {
            if let Some(socket_addr) = self.queued_peers.pop_front() {
                let task_id = self.next_id;
                self.next_id += 1;

                tracing::debug!("Created a new peer task with id of '{}'", task_id);

                let peer = Peer::create(
                    task_id,
                    socket_addr,
                    Arc::clone(&handshake),
                    Arc::clone(&self.metainfo),
                    self.pm_sender.clone(),
                    self.appstate_recv.clone(),
                )
                .await;

                let completion_sender = completion_sender.clone();

                let peer_task = tokio::spawn(async move {
                    if let Err(e) = peer.start().await {
                        tracing::debug!("Peer task error: '{:?}'", e);
                    }

                    completion_sender.send(task_id).await.expect(
                        "Internal error: a peer task couldn't send a completion \
                            notification to the tracker task",
                    );
                });

                self.active_peers.insert(task_id, peer_task);
            }
        }

        Ok(())
    }

    fn gen_client_id() -> Arc<[u8; 20]> {
        let mut id = [0u8; 20];
        id[..8].copy_from_slice("-LO0001-".as_bytes());

        for i in 0..12 {
            id[8 + i] = rand::random::<u8>();
        }

        Arc::new(id)
    }
}

struct TrackerTask {
    url: String,
    metainfo: Arc<Metainfo>,
    client_id: Arc<[u8; 20]>,
    tracker_resp_sender: mpsc::Sender<Vec<SocketAddrV4>>,
    appstate_recv: watch::Receiver<AppState>,
}

impl TrackerTask {
    fn new(
        url: String,
        metainfo: Arc<Metainfo>,
        client_id: Arc<[u8; 20]>,
        tracker_resp_sender: mpsc::Sender<Vec<SocketAddrV4>>,
        appstate_recv: watch::Receiver<AppState>,
    ) -> Self {
        Self {
            url,
            metainfo,
            client_id,
            tracker_resp_sender,
            appstate_recv,
        }
    }

    async fn start(mut self) -> TrResult<()> {
        // TODO: proper timeouts and reannounces
        let mut protocol = TrackerProtocol::init(&self.url).await?;

        let response_timeout = tokio::time::timeout(
            Duration::from_secs(30),
            protocol.announce(&self.metainfo, &self.client_id),
        );

        tokio::select! {
            _ = self.appstate_recv.changed() => {
                if let AppState::Exit = *self.appstate_recv.borrow() {
                    return Ok(());
                }
            }
            response_timeout = response_timeout => {
                let response = match response_timeout {
                    Ok(r) => r?,
                    Err(_) => return Ok(()),
                };

                tracing::info!(
                    "Received '{}' peers from a tracker at '{}'",
                    response.peers.len(),
                    &self.url
                );

                self.tracker_resp_sender.send(response.peers).await.expect(
                    "Internal error: a tracker task couldn't send a TrackerResponse to the Tracker Manager",
                );
            }
        }

        Ok(())
    }
}

const DEFAULT_PORT: u16 = 6881;

enum TrackerProtocol<'u> {
    Http(HttpTracker<'u>),
    Udp(UdpTracker),
}

impl<'u> TrackerProtocol<'u> {
    async fn init(url: &'u str) -> TrResult<TrackerProtocol<'_>> {
        match url.split_once("://") {
            Some(("http", _)) => Ok(Self::Http(HttpTracker::init(url))),
            Some(("udp", _)) => Ok(Self::Udp(UdpTracker::init(url).await?)),
            _ => return Err(TrErr::UnknownProtocol),
        }
    }

    async fn announce(
        &mut self,
        metainfo: &Metainfo,
        client_id: &[u8; 20],
    ) -> TrResult<TrackerResponse> {
        match self {
            TrackerProtocol::Http(http_tracker) => http_tracker.announce(metainfo, client_id).await,
            TrackerProtocol::Udp(udp_tracker) => udp_tracker.announce(metainfo, client_id).await,
        }
    }
}

pub struct TrackerResponse {
    /// Interval in seconds that the client should wait between sending regular requests to the tracker
    interval: u32,
    /// number of peers with the entire file
    seeds: Option<u32>,
    /// number of non-seeder peers
    leeches: Option<u32>,
    /// IP addresses and ports of peers (currently only the binary model is supported)
    pub peers: Vec<SocketAddrV4>,
}

impl TrackerResponse {
    pub fn new(
        interval: u32,
        seeds: Option<u32>,
        leeches: Option<u32>,
        peers: Vec<SocketAddrV4>,
    ) -> Self {
        Self {
            interval,
            seeds,
            leeches,
            peers,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u32)]
enum ClientState {
    // Completed = 1
    //Paused,
    Started = 2,
    //Stopped = 3,
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

impl std::fmt::Debug for TrackerResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrackerResponse")
            .field("interval", &self.interval)
            .field("seeds", &self.seeds)
            .field("leeches", &self.leeches)
            .field("peers", &format_args!("<peer IP addresses>"))
            .finish()
    }
}

type TrResult<T> = Result<T, TrErr>;

#[derive(Error, Debug)]
pub enum TrErr {
    #[error("Bencoding response parse error: '{0}'")]
    BeError(#[from] ResponseParseError),
    #[error("Bencode decoding error while parsing the tracker response: '{0}'")]
    BeDecodeError(#[from] BeDecodeErr),
    #[error("Peers string length '{0}' should be a multiple of 6")]
    InvalidPeersLen(usize),
    #[error("UDP Tracker message parse error: '{0}'")]
    TrackerMsgErr(#[from] TrackerMsgDecodeErr),
    #[error("Received an invalid message from a UDP tracker")]
    TrackerProtocolErr,

    #[error("Failed HTTP request to the tracker: '{0}'")]
    HttpErr(#[from] reqwest::Error),
    #[error("General IO error: '{0}'")]
    Io(#[from] std::io::Error),

    #[error("Unknown tracker protocol")]
    UnknownProtocol,
    #[error("Invalid tracker URL")]
    InvalidUrl,

    #[error("Tokio join error: '{0}'")]
    JoinError(#[from] tokio::task::JoinError),
}
