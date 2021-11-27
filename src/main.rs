use std::{net::SocketAddrV4, sync::Arc};

use futures_util::stream::StreamExt;
use message::{Message, MessageCodec, MessageDecoderErr};
use thiserror::Error;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::error::Elapsed,
};
use tokio_util::codec::Framed;

use crate::{bencoding::bevalue::BeValue, tracker::ClientState};
use protocol::Handshake;

mod bencoding;
mod message;
mod metainfo;
mod protocol;
mod tracker;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let file_contents = fs::read("raspios-2021-10-30-bullseye-armhf.zip.torrent")
        .await
        .unwrap();
    let contents = BeValue::from_bytes(&file_contents).unwrap();

    let metainfo = metainfo::MetaInfo::from_src_be(&file_contents, contents).unwrap();

    tracing::info!("Parsed torrent metainfo");

    let client_id = tracker::gen_client_id();
    let req_url = tracker::build_announce_url(
        &metainfo,
        &client_id,
        0,
        0,
        metainfo.total_length,
        ClientState::Started,
    );

    let response = reqwest::get(req_url).await.unwrap().bytes().await.unwrap();
    let response = BeValue::from_bytes(&response).unwrap();
    let response = tracker::TrackerResponse::from_bevalue(response).unwrap();

    tracing::info!(
        "Parsed tracker response. Peers: {:?}",
        &response.peers.len()
    );

    let handshake = Arc::new(Handshake::new(&client_id, &metainfo));

    let mut tasks = Vec::new();

    for socket_addr in response.peers {
        let handshake = Arc::clone(&handshake);
        tasks.push(tokio::spawn(async move {
            match process(socket_addr, handshake).await {
                Ok(_) => (),
                Err(e) => {
                    tracing::error!("Torrent error: '{:?}'", e);
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

type PeerConnection = Framed<TcpStream, MessageCodec>;

async fn process(socket_addr: SocketAddrV4, handshake: Arc<Handshake>) -> Result<(), TrError> {
    let mut stream = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        TcpStream::connect(socket_addr),
    )
    .await??;

    tracing::info!(
        "Successfull connection: '{:?}'. Sending a handshake.",
        socket_addr
    );

    stream.write_all(&handshake.inner).await?;

    let mut peer_handshake = Handshake::new_empty();
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        stream.read_exact(&mut peer_handshake.inner).await
    })
    .await??;

    tracing::info!("Received a handshake from: '{:?}'", socket_addr);
    let peer_id = peer_handshake.validate(&peer_handshake)?;
    tracing::info!(
        "Good handshake. Peer: '{:?}'",
        String::from_utf8_lossy(&peer_id)
    );

    let mut peer_con = PeerConnection::new(stream, MessageCodec);

    let mut am_choking = true;
    let mut am_interested = false;
    let mut peer_choking = true;
    let mut peer_interested = false;

    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let msg = receive_message(&mut peer_con, 20).await;
            tracing::info!("Received a message: '{:?}'", msg);
        }
    })
    .await?;

    Ok(())
}

async fn receive_message(peer_con: &mut PeerConnection, timeout: u64) -> Result<Message, TrError> {
    let res = tokio::time::timeout(std::time::Duration::from_secs(timeout), async {
        loop {
            let res = peer_con.next().await;
            if let Some(inner) = res {
                break inner;
            }
        }
    })
    .await;

    res?.map_err(|e| TrError::InvalidMessage(e))
}

#[derive(Error, Debug)]
pub enum TrError {
    #[error("TCP error: '{0}'")]
    FailedConnction(#[from] tokio::io::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("Received an invalid handshake")]
    InvalidHandshake,
    #[error("Received an invalid message: '{0}'")]
    InvalidMessage(#[from] MessageDecoderErr),
}
