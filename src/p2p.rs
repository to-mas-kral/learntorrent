use std::{net::SocketAddrV4, sync::Arc, time::Duration};

use flume::{Receiver, Sender};
use futures_util::StreamExt;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_util::codec::Framed;

use crate::{
    message::{Message, MessageCodec},
    piece_manager::{PieceId, PmMessage, TaskId},
    protocol::Handshake,
    TrError,
};

type MsgStream = Framed<TcpStream, MessageCodec>;

pub struct PeerTask {
    id: TaskId,
    socket_addr: SocketAddrV4,
    handshake: Arc<Handshake>,
    pm_tx: Sender<PmMessage>,
    piece_rx: Receiver<PieceId>,
}

impl PeerTask {
    pub fn new(
        id: TaskId,
        socket_addr: SocketAddrV4,
        handshake: Arc<Handshake>,
        pm_sender: Sender<PmMessage>,
        piece_rx: Receiver<PieceId>,
    ) -> Self {
        Self {
            id,
            socket_addr,
            handshake,
            pm_tx: pm_sender,
            piece_rx,
        }
    }
}

pub async fn process(pt: PeerTask) -> Result<(), TrError> {
    let mut stream = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        TcpStream::connect(pt.socket_addr),
    )
    .await??;

    tracing::trace!(
        "Successfull connection: '{:?}'. Sending a handshake.",
        pt.socket_addr
    );

    stream.write_all(&pt.handshake.inner).await?;

    let mut peer_handshake = Handshake::new_empty();
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        stream.read_exact(&mut peer_handshake.inner).await
    })
    .await??;

    let peer_id = peer_handshake.validate(&peer_handshake)?;
    let mut msg_stream = MsgStream::new(stream, MessageCodec);

    // let mut am_choking = true;
    // let mut am_interested = false;
    // let mut peer_choking = true;
    // let mut peer_interested = false;

    tracing::info!("Handshake with '{:?}' complete", pt.socket_addr);

    // A bitfield message may only be sent immediately after the handshake
    let msg = receive_message(&mut msg_stream, 20).await?;

    /* pt.pm_tx
        .send_async(PmMessage::Request(pt.id))
        .await
        .map_err(|_| TrError::CatchAll)?; */

    //tokio::time::timeout(std::time::Duration::from_secs(20), async { loop {} }).await?;

    Ok(())
}

async fn receive_message(peer_con: &mut MsgStream, timeout: u64) -> Result<Message, TrError> {
    let res = tokio::time::timeout(Duration::from_secs(timeout), async {
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
