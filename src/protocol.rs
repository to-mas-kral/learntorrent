use crate::{metainfo::MetaInfo, TrError};

pub struct Handshake {
    pub inner: [u8; 68],
}

impl Handshake {
    pub fn new(client_id: &[u8], metainfo: &MetaInfo) -> Self {
        let mut handshake = [0; 68];
        handshake[0] = 0x13;
        handshake[1..20].copy_from_slice("BitTorrent protocol".as_bytes());
        handshake[20..28].fill(0);
        handshake[28..48].copy_from_slice(&metainfo.info_hash);
        handshake[48..68].copy_from_slice(client_id);

        Handshake { inner: handshake }
    }

    pub fn new_empty() -> Self {
        Handshake { inner: [0; 68] }
    }

    pub fn validate<'p>(&self, peer: &'p Self) -> Result<&'p [u8], TrError> {
        if self.inner[0..48] != peer.inner[0..48] {
            return Err(TrError::InvalidHandshake);
        }

        return Ok(&peer.inner[48..68]);
    }
}
