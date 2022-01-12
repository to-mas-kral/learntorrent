use std::sync::Arc;

use eyre::{Result, WrapErr};
use tokio::fs;

use crate::{
    bencoding::bevalue::BeValue,
    io::Io,
    metainfo::Metainfo,
    p2p::{Handshake, PeerTask},
    piece_manager::PieceManager,
    tracker::{ClientState, TrackerResponse},
};

mod bencoding;
mod io;
mod metainfo;
mod p2p;
mod piece_manager;
mod tracker;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let file_contents = fs::read("debian-11.2.0-amd64-netinst.iso.torrent")
        .await
        .wrap_err("Failed to read the torrent metadata file")?;

    let contents =
        BeValue::from_bytes(&file_contents).wrap_err("Failed to parse the torrent metadata")?;
    let metainfo = Metainfo::from_src_be(&file_contents, contents)
        .wrap_err("Failed to create the metainfo struct")?;

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
    let response = reqwest::get(req_url)
        .await
        .wrap_err("Failed to complete the HTTP GET request to the tracker")?
        .bytes()
        .await
        .wrap_err("Failed to complete the HTTP GET request to the tracker")?;
    let response =
        BeValue::from_bytes(&response).wrap_err("Failed to parse the tracker response")?;
    let response = TrackerResponse::from_bevalue(response)
        .wrap_err("Failed to create the tracker response struct")?;

    tracing::debug!(
        "Parsed tracker response. Peers: {:?}",
        &response.peers.len()
    );

    let handshake = Arc::new(Handshake::new(&client_id, &metainfo));

    let (io, io_sender) = Io::new(Arc::clone(&metainfo));
    let (pm, pm_sender, register_recv, notify_recv) =
        PieceManager::new(metainfo.piece_count(), io_sender.clone());

    let pm_task = tokio::spawn(PieceManager::piece_manager(pm));
    let io_task = tokio::spawn(Io::start(io));

    let mut peer_tasks = Vec::new();
    for socket_addr in response.peers {
        let handshake = Arc::clone(&handshake);
        let metainfo = Arc::clone(&metainfo);
        let pm_sender = pm_sender.clone();
        let register_recv = register_recv.clone();
        let notify_recv = notify_recv.clone();

        peer_tasks.push(tokio::spawn(async move {
            let res = PeerTask::create(
                socket_addr,
                handshake,
                metainfo,
                pm_sender,
                register_recv,
                notify_recv,
            )
            .await;

            if let Err(e) = &res {
                tracing::debug!("Peer task error: '{:?}'", e);
            }

            res
        }));
    }

    for t in peer_tasks {
        if let Err(e) = t.await {
            tracing::error!("Task falied to complete: '{}'", e)
        };
    }

    pm_task.await??;
    io_task.await??;

    Ok(())
}
