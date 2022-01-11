use std::sync::Arc;

use tokio::fs;

use bencoding::bevalue::BeValue;
use io::Io;
use p2p::{Handshake, PeerTask};
use piece_manager::PieceManager;
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
        .with_max_level(tracing::Level::DEBUG)
        .init();

    //let file_contents = fs::read("ubuntu-mate-21.10-desktop-amd64.iso.torrent")
    let file_contents = fs::read("debian-11.2.0-amd64-netinst.iso.torrent")
        .await
        .expect("Couldn't read the torrent file");
    let contents = BeValue::from_bytes(&file_contents).expect("Couldn't parse the torrent file");
    let metainfo = metainfo::Metainfo::from_src_be(&file_contents, contents)
        .expect("Couldn't parse the torrent file");

    tracing::debug!("Torrent metainfo: {:?}", metainfo);

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

    let (io, io_sender) = Io::new(Arc::clone(&metainfo));
    let (pm, pm_sender, register_recv, notify_recv) =
        PieceManager::new(metainfo.piece_count(), io_sender.clone());

    let mut tasks = vec![
        tokio::spawn(PieceManager::piece_manager(pm)),
        tokio::spawn(Io::start(io)),
    ];

    for socket_addr in response.peers {
        let handshake = Arc::clone(&handshake);
        let metainfo = Arc::clone(&metainfo);
        let pm_sender = pm_sender.clone();
        let register_recv = register_recv.clone();
        let notify_recv = notify_recv.clone();

        tasks.push(tokio::spawn(async move {
            if let Err(e) = PeerTask::create(
                socket_addr,
                handshake,
                metainfo,
                pm_sender,
                register_recv,
                notify_recv,
            )
            .await
            {
                tracing::debug!("Peer task error: '{:?}'", e);
            }
        }));
    }

    for t in tasks {
        if let Err(e) = t.await {
            tracing::error!("Task falied to complete: '{}'", e)
        };
    }
}
