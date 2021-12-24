use crate::{metainfo::MetaInfo, TrError};

pub struct Handshake {
    pub inner: Vec<u8>,
}

impl Handshake {
    const HANDSHAKE_LEN: usize = 68;

    pub fn new(client_id: &[u8], metainfo: &MetaInfo) -> Self {
        let mut handshake = vec![0; Self::HANDSHAKE_LEN];
        handshake[0] = 0x13;
        handshake[1..20].copy_from_slice("BitTorrent protocol".as_bytes());
        // Extensions, currently all 0
        handshake[20..28].fill(0);
        handshake[28..48].copy_from_slice(&metainfo.info_hash);
        handshake[48..68].copy_from_slice(client_id);

        Handshake { inner: handshake }
    }

    pub fn new_empty() -> Self {
        Handshake {
            inner: vec![0; Self::HANDSHAKE_LEN],
        }
    }

    /// Check if info hash matches, then returns the peer id
    pub fn validate<'p>(&self, peer: &'p Self) -> Result<&'p [u8], TrError> {
        if self.inner[0..48] != peer.inner[0..48] {
            return Err(TrError::InvalidHandshake);
        }

        return Ok(&peer.inner[48..68]);
    }
}
