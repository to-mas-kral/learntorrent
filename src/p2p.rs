use std::{net::SocketAddrV4, sync::Arc, time::Duration};

use flume::{Receiver, Sender};
use futures::sink::SinkExt;
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
        std::time::Duration::from_secs(120),
        TcpStream::connect(pt.socket_addr),
    )
    .await??;

    tracing::trace!(
        "Successfull connection: '{:?}'. Sending a handshake.",
        pt.socket_addr
    );

    stream.write_all(&pt.handshake.inner).await?;

    let mut peer_handshake = Handshake::new_empty();
    tokio::time::timeout(std::time::Duration::from_secs(120), async {
        stream.read_exact(&mut peer_handshake.inner).await
    })
    .await??;

    let peer_id = peer_handshake.validate(&peer_handshake)?;
    let mut msg_stream = MsgStream::new(stream, MessageCodec);

    tracing::info!("Handshake with '{:?}' complete", pt.socket_addr);

    let mut am_choked = true;
    let mut am_interested = false;
    // let mut peer_choked = true;
    // let mut peer_interested = false;

    let mut first_message = true;
    loop {
        // A bitfield message may only be sent immediately after the handshake
        // TODO: if requesting piece, then let piece manager know that the request failed
        let msg = receive_message(&mut msg_stream, 120).await?;

        match msg {
            Message::KeepAlive => continue,
            Message::Choke => am_choked = true,
            Message::Unchoke => am_choked = false,
            Message::Have(piece_id) => {
                pt.pm_tx
                    .send_async(PmMessage::Have(pt.id, piece_id))
                    .await?;
            }
            Message::Bitfield(bitfield) => {
                if first_message {
                    // TODO: verify the bitfield (length)
                    pt.pm_tx
                        .send_async(PmMessage::HaveBitfield(pt.id, bitfield))
                        .await?
                } else {
                    // TODO: drop the connection after receiving bitfield as a non-first message ?
                }
            }
            Message::Piece {
                index,
                begin,
                block,
            } => {
                tracing::info!("Received a piece from peer");
            }
            m => tracing::warn!("Unimplmeneted message received: {:?}", m),
        }

        first_message = false;

        if !am_interested && am_choked {
            am_interested = true;
            msg_stream
                .send(Message::Interested)
                .await
                .expect("Should never happen");
        }

        if !am_choked {
            pt.pm_tx.send_async(PmMessage::Pick(pt.id)).await?;
            let piece_id = pt.piece_rx.recv_async().await?;

        }
    }
}

async fn receive_message(msg_stream: &mut MsgStream, timeout: u64) -> Result<Message, TrError> {
    let res = tokio::time::timeout(Duration::from_secs(timeout), async {
        loop {
            let res = msg_stream.next().await;
            if let Some(inner) = res {
                break inner;
            }
        }
    })
    .await;

    res?.map_err(TrError::InvalidMessage)
}
