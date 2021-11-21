use std::net::{Ipv4Addr, SocketAddrV4};

use thiserror::Error;

use crate::{
    bencoding::bevalue::{BeValue, ResponseParseError},
    metainfo::MetaInfo,
};

pub fn build_announce_url(
    mi: &MetaInfo,
    client_id: &[u8],
    uploaded: u64,
    downloaded: u64,
    left: u64,
    event: ClientState,
) -> String {
    let port = 6881;

    return format!(
        "{announce}?info_hash={info_hash}&peer_id={peer_id}&port={port}&uploaded={uploaded}&downloaded={downloaded}&left={left}&event={event}&compact=1",
        announce = mi.announce,
        info_hash = urlencoding::encode_binary(&mi.info_hash),
        peer_id = urlencoding::encode_binary(client_id),
        port = port,
        uploaded = uploaded,
        downloaded = downloaded,
        left = left,
        event = event.to_str(),
    );
}

pub fn gen_client_id() -> Vec<u8> {
    let mut id = Vec::with_capacity(20);
    id.extend_from_slice("-LO0001-".as_bytes());

    for _ in 0..12 {
        id.push(rand::random::<u8>());
    }

    id
}

#[derive(Debug)]
pub struct TrackerResponse {
    min_interval: u64,
    interval: u64,
    complete: u64,
    incomplete: u64,
    pub peers: Vec<SocketAddrV4>,
}

impl TrackerResponse {
    pub fn from_bevalue(mut be: BeValue) -> TrResult<Self> {
        let mut resp = be.take_dict()?;

        let min_interval = resp.expect("min interval")?.take_uint()?;
        let interval = resp.expect("interval")?.take_uint()?;
        let complete = resp.expect("complete")?.take_uint()?;
        let incomplete = resp.expect("incomplete")?.take_uint()?;
        let peers = resp.expect("peers")?.take_str()?;

        if peers.len() % 6 != 0 {
            return Err(TrError::InvalidPeersLen(peers.len()));
        }
        // TODO: validate addresses ?
        let peers = peers
            .chunks(6)
            .map(|c| {
                SocketAddrV4::new(
                    Ipv4Addr::new(c[0], c[1], c[2], c[3]),
                    u16::from_be_bytes([c[4], c[5]]),
                )
            })
            .collect();

        Ok(TrackerResponse {
            min_interval,
            interval,
            complete,
            incomplete,
            peers,
        })
    }
}

type TrResult<T> = Result<T, TrError>;

#[derive(Error, Debug)]
pub enum TrError {
    #[error("{0}")]
    BeError(#[from] ResponseParseError),
    #[error("Peers length '{0}' should be a multiple of 6")]
    InvalidPeersLen(usize),
}

#[derive(Clone, Copy)]
pub enum ClientState {
    Started,
    Stopped,
    Paused,
}

impl ClientState {
    pub fn to_str(self) -> &'static str {
        match self {
            ClientState::Started => "started",
            ClientState::Stopped => "stopped",
            ClientState::Paused => "paused",
        }
    }
}

#[cfg(test)]
mod test_super {
    #[test]
    fn test_urlencoding() {
        assert_eq!(
            urlencoding::encode_binary(&[
                0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf1, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd,
                0xef, 0x12, 0x34, 0x56, 0x78, 0x9a
            ]),
            "%124Vx%9A%BC%DE%F1%23Eg%89%AB%CD%EF%124Vx%9A"
        );
    }
}
