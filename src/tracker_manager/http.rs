use std::net::{Ipv4Addr, SocketAddrV4};

use super::{ClientState, TrErr, TrResult, TrackerResponse, DEFAULT_PORT};
use crate::{
    bencoding::bevalue::{BeStr, BeValue},
    metainfo::Metainfo,
};

pub struct HttpTracker<'u> {
    /// A string that the client should send back on its next announcements.
    /// If absent and a previous announce sent a tracker id, do not discard the old value; keep using it.
    tracker_id: Option<BeStr>,
    /// The tracker URL without the prefix and suffix
    url: &'u str,
}

impl<'u> HttpTracker<'u> {
    pub fn init(url: &'u str) -> Self {
        Self {
            tracker_id: None,
            url,
        }
    }

    pub async fn announce(
        &mut self,
        metainfo: &Metainfo,
        client_id: &[u8; 20],
    ) -> TrResult<TrackerResponse> {
        let req_url = self.build_announce_url(
            metainfo,
            client_id,
            0,
            0,
            metainfo.piece_count(),
            ClientState::Started,
        );

        let response = reqwest::get(req_url).await?.bytes().await?;
        let response = BeValue::from_bytes(&response)?;
        let response = self.parse_bevalue(response)?;

        Ok(response)
    }

    fn build_announce_url(
        &self,
        metainfo: &Metainfo,
        client_id: &[u8],
        uploaded: u32,
        downloaded: u32,
        left: u32,
        event: ClientState,
    ) -> String {
        return format!(
            "{announce}?info_hash={info_hash}&peer_id={peer_id}&port={port}&uploaded={uploaded}&downloaded={downloaded}&left={left}&event={event}&compact=1",
            announce = self.url,
            info_hash = urlencoding::encode_binary(&*metainfo.info_hash),
            peer_id = urlencoding::encode_binary(client_id),
            port = DEFAULT_PORT,
            uploaded = uploaded,
            downloaded = downloaded,
            left = left,
            event = event.to_str(),
        );
    }

    fn parse_bevalue(&mut self, mut be: BeValue) -> TrResult<TrackerResponse> {
        let resp = be.get_dict()?;

        let interval = resp.expect("interval")?.get_u32()?;

        let seeds = resp.try_get("complete", BeValue::get_u32)?;
        let leeches = resp.try_get("incomplete", BeValue::get_u32)?;
        let peers = resp.expect("peers")?.get_str()?;
        if peers.len() % 6 != 0 {
            return Err(TrErr::InvalidPeersLen(peers.len()));
        }

        let peers = peers
            .chunks(6)
            .map(|c| {
                SocketAddrV4::new(
                    Ipv4Addr::new(c[0], c[1], c[2], c[3]),
                    u16::from_be_bytes([c[4], c[5]]),
                )
            })
            .collect();

        let tracker_id = resp.try_get("tracker id", BeValue::get_str)?;
        if tracker_id.is_some() {
            self.tracker_id = tracker_id;
        }

        Ok(TrackerResponse {
            interval,
            seeds,
            leeches,
            peers,
        })
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
