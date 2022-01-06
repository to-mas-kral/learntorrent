use std::sync::Arc;

use message::MessageDecoderErr;
use thiserror::Error;
use tokio::{fs, time::error::Elapsed};

use bencoding::bevalue::BeValue;
use p2p::PeerTask;
use piece_manager::{PieceManager, PmMessage};
use protocol::Handshake;
use tracker::ClientState;

mod bencoding;
mod message;
mod metainfo;
mod p2p;
mod piece_manager;
mod protocol;
mod tracker;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .init();

    let file_contents = fs::read("raspios-2021-10-30-bullseye-armhf.zip.torrent")
        .await
        .unwrap();
    let contents = BeValue::from_bytes(&file_contents).unwrap();

    let metainfo = metainfo::MetaInfo::from_src_be(&file_contents, contents).unwrap();

    tracing::trace!("Parsed torrent metainfo");

    let client_id = tracker::gen_client_id();
    let req_url = tracker::build_announce_url(
        &metainfo,
        &client_id,
        0,
        0,
        metainfo.total_length,
        ClientState::Started,
    );

    // TODO: periodically resend the announce request
    // TODO: manage a good number (20-30?) of active peers
    let response = reqwest::get(req_url).await.unwrap().bytes().await.unwrap();
    let response = BeValue::from_bytes(&response).unwrap();
    let response = tracker::TrackerResponse::from_bevalue(response).unwrap();

    tracing::trace!(
        "Parsed tracker response. Peers: {:?}",
        &response.peers.len()
    );

    let handshake = Arc::new(Handshake::new(&client_id, &metainfo));

    let (pm, pm_tx, register_rx) = PieceManager::new(metainfo.num_pieces());

    let mut tasks = Vec::new();

    tasks.push(tokio::spawn(PieceManager::piece_manager(pm)));

    for socket_addr in response.peers {
        let handshake = Arc::clone(&handshake);
        let pm_sender = pm_tx.clone();

        pm_tx
            .send_async(PmMessage::Register)
            .await
            .expect("Shouldn't panic ?");
        let (task_id, piece_rx) = register_rx.recv_async().await.expect("Shouldn't panic ?");

        let peer_task = PeerTask::new(task_id, socket_addr, handshake, pm_sender, piece_rx);

        tasks.push(tokio::spawn(async move {
            match p2p::process(peer_task).await {
                Ok(_) => (),
                Err(e) => {
                    tracing::info!("{:?}", e);
                }
            };
        }));
    }

    for t in tasks {
        match t.await {
            Ok(_) => (),
            Err(e) => tracing::error!("Task falied to complete: '{}'", e),
        };
    }
}

#[derive(Error, Debug)]
pub enum TrError {
    #[error("TCP error: '{0}'")]
    FailedConnection(#[from] tokio::io::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("Received an invalid handshake")]
    InvalidHandshake,
    #[error("Received an invalid message: '{0}'")]
    InvalidMessage(#[from] MessageDecoderErr),
    #[error("Flume error")]
    FlumeErr(#[from] flume::SendError<PmMessage>),
    #[error("Flume error")]
    FlumeErr2(#[from] flume::RecvError),
}
