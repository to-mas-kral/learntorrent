use std::sync::Arc;

use eyre::{Result, WrapErr};
use tokio::fs;

use crate::{
    bencoding::bevalue::BeValue, io::Io, metainfo::Metainfo, piece_manager::PieceManager,
    tracker::Tracker,
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
        .with_max_level(tracing::Level::ERROR)
        .init();

    let file_contents = fs::read("debian-11.2.0-amd64-netinst.iso.torrent")
        .await
        .wrap_err("Failed to read the torrent metadata file")?;

    let contents =
        BeValue::from_bytes(&file_contents).wrap_err("Failed to parse the torrent metadata")?;

    let metainfo = Metainfo::from_src_be(&file_contents, contents)
        .wrap_err("Failed to create the metainfo struct")?;

    tracing::debug!("Torrent metainfo parsed: {:?}", metainfo);

    let metainfo = Arc::new(metainfo);

    let (io, io_sender) = Io::new(Arc::clone(&metainfo));
    let (pm, pm_sender, notify_recv) = PieceManager::new(metainfo.piece_count(), io_sender.clone());

    let tracker = Tracker::new(Arc::clone(&metainfo), pm_sender, notify_recv);
    let tracker_task = tokio::spawn(Tracker::start(tracker));

    let pm_task = tokio::spawn(PieceManager::start(pm));
    let io_task = tokio::spawn(Io::start(io));

    pm_task.await??;
    io_task.await??;
    tracker_task.await??;

    Ok(())
}
