use std::{env, sync::Arc};

use eyre::{Result, WrapErr};
use tokio::{
    fs,
    sync::{mpsc, watch},
};

use crate::{
    bencoding::bevalue::BeValue, io::Io, metainfo::Metainfo, piece_keeper::PieceKeeper,
    tracker_manager::TrackerManager,
};

mod bencoding;
mod io;
mod metainfo;
mod p2p;
mod piece_keeper;
mod tracker_manager;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let metadata_path = env::args()
        .nth(1)
        .unwrap_or("debian-11.2.0-amd64-netinst.iso.torrent".to_string());

    let file_contents = fs::read(metadata_path)
        .await
        .wrap_err("Failed to read the torrent metadata file")?;

    let contents =
        BeValue::from_bytes(&file_contents).wrap_err("Failed to parse the torrent metadata")?;

    let metainfo = Metainfo::from_src_be(&file_contents, contents)
        .wrap_err("Failed to create the metainfo struct")?;

    tracing::debug!("Torrent metainfo parsed: {:?}", metainfo);
    let metainfo = Arc::new(metainfo);

    let (appstate_sender, appstate_recv) = watch::channel(AppState::Running);
    let (io_sender, io_recv) = mpsc::channel(128);
    let (pm_sender, pm_recv) = flume::bounded(128);

    let io = Io::new(io_recv, Arc::clone(&metainfo))
        .await
        .wrap_err("Couldn't initialize the I/O task")?;

    let pm = PieceKeeper::new(
        appstate_recv.clone(),
        pm_recv.clone(),
        io_sender.clone(),
        metainfo.piece_count(),
    );
    let tracker = TrackerManager::new(
        appstate_recv.clone(),
        pm_sender.clone(),
        Arc::clone(&metainfo),
    );

    drop(appstate_recv);
    drop(io_sender);
    drop(pm_sender);
    drop(pm_recv);

    let tracker_task = tokio::spawn(TrackerManager::start(tracker));
    let pm_task = tokio::spawn(PieceKeeper::start(pm));
    let io_task = tokio::spawn(Io::start(io));

    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            appstate_sender
                .send(AppState::Exit)
                .expect("Main Task failed to send appstate notification to subtasks");
        }
        Err(err) => {
            panic!("Unable to listen for shutdown signal: {}", err);
        }
    }

    pm_task.await?;
    io_task.await??;
    tracker_task.await??;

    Ok(())
}

#[derive(Debug)]
pub enum AppState {
    Running,
    Pause,
    Exit,
}
