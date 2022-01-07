use std::sync::Arc;

use tokio::fs;

use bencoding::bevalue::BeValue;
use io::Io;
use p2p::{Handshake, PeerTask};
use piece_manager::{PieceManager, PmMsg};
use tracker::ClientState;

mod bencoding;
mod io;
mod metainfo;
mod p2p;
mod piece_manager;
mod tracker;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .init();

    //let file_contents = fs::read("raspios-2021-10-30-bullseye-armhf.zip.torrent")
    let file_contents = fs::read("2021-10-30-raspios-bullseye-armhf.zip.torrent")
        .await
        .unwrap();
    let contents = BeValue::from_bytes(&file_contents).unwrap();

    let metainfo = metainfo::MetaInfo::from_src_be(&file_contents, contents).unwrap();

    tracing::debug!("Parsed torrent metainfo");

    let client_id = tracker::gen_client_id();
    let req_url = tracker::build_announce_url(
        &metainfo,
        &client_id,
        0,
        0,
        metainfo.total_length,
        ClientState::Started,
    );

    let metainfo = Arc::new(metainfo);

    // TODO: periodically resend the announce request
    // TODO: manage a good number (20-30?) of active peers
    let response = reqwest::get(req_url).await.unwrap().bytes().await.unwrap();
    let response = BeValue::from_bytes(&response).unwrap();
    let response = tracker::TrackerResponse::from_bevalue(response).unwrap();

    tracing::debug!(
        "Parsed tracker response. Peers: {:?}",
        &response.peers.len()
    );

    let handshake = Arc::new(Handshake::new(&client_id, &metainfo));

    let (pm, pm_tx, register_rx) = PieceManager::new(metainfo.num_pieces());
    let (io, io_tx) = Io::new(Arc::clone(&metainfo));

    let mut tasks = Vec::new();

    tasks.push(tokio::spawn(PieceManager::piece_manager(pm)));
    tasks.push(tokio::spawn(Io::start(io)));

    for socket_addr in response.peers {
        let handshake = Arc::clone(&handshake);
        let pm_sender = pm_tx.clone();

        pm_tx
            .send_async(PmMsg::Register)
            .await
            .expect("Shouldn't panic ?");
        let (task_id, piece_rx) = register_rx.recv_async().await.expect("Shouldn't panic ?");

        let peer_task = PeerTask::new(
            task_id,
            socket_addr,
            handshake,
            pm_sender,
            piece_rx,
            io_tx.clone(),
            Arc::clone(&metainfo),
        );

        tasks.push(tokio::spawn(async move {
            match PeerTask::create(peer_task).await {
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
