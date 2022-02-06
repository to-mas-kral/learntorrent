use std::net::{Ipv4Addr, SocketAddrV4};

use bytes::Buf;
use thiserror::Error;
use tokio::net::UdpSocket;

use crate::metainfo::Metainfo;

use super::{ClientState, TrErr, TrResult, TrackerResponse};

pub struct UdpTracker {
    /// Connection ID received in the first response
    conn_id: Option<u64>,
    /// Socket for communicating with the tracker
    socket: UdpSocket,
}

impl UdpTracker {
    pub async fn init(url: &str) -> TrResult<Self> {
        let url = url
            .trim_start_matches("udp://")
            .split_once("/announce")
            .ok_or(TrErr::InvalidUrl)?
            .0;

        // "It's not obvious, but if you bind the socket to (UNSPECIFIED, 0),
        // then when you connect to a remote address the local address is set to
        // the appropriate interface address. (Port 0 means to auto-allocate a port,
        // just like an unbound socket in C.)"
        let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
        socket.connect(&url).await?;

        let sel = Self {
            conn_id: None,
            socket,
        };

        Ok(sel)
    }

    /// "2 kilobytes should be enough for everyone"
    /// (enough for exactly 338 peers)
    const MAXIMUM_PACKET_SIZE: usize = 2048;

    pub async fn announce(
        &mut self,
        metainfo: &Metainfo,
        client_id: &[u8; 20],
    ) -> TrResult<TrackerResponse> {
        let mut read_buffer = vec![0; Self::MAXIMUM_PACKET_SIZE];

        let (connect_msg, trans_id) = TrackerRequestMsg::new_connect();

        // TODO: retries and timeouts
        self.send_msg(connect_msg).await?;
        let connect_resp = self.recv_msg(&mut read_buffer).await?;
        let connect_resp = connect_resp.expect_conncet()?;
        self.check_trans_id(trans_id, connect_resp.trans_id)?;

        self.conn_id = Some(connect_resp.conn_id);

        let port = self.socket.local_addr()?.port();
        let (announce_msg, trans_id) = TrackerRequestMsg::new_announce(
            self.conn_id.unwrap(),
            &*metainfo.info_hash,
            client_id,
            0,
            metainfo.total_length,
            0,
            ClientState::Started,
            port,
        );

        self.send_msg(announce_msg).await?;
        let announce_resp = self.recv_msg(&mut read_buffer).await?;
        let ar = announce_resp.expect_announce()?;
        self.check_trans_id(trans_id, ar.trans_id)?;

        let tracker_response =
            TrackerResponse::new(ar.interval, Some(ar.seeders), Some(ar.leechers), ar.peers);

        Ok(tracker_response)
    }

    async fn recv_msg(&self, buf: &mut [u8]) -> TrResult<TrackerResponseMsg> {
        let bytes_read = self.socket.recv(buf).await?;

        if bytes_read >= Self::MAXIMUM_PACKET_SIZE {
            tracing::warn!("Received UDP packet might have been bigger than the max size");
        }

        Ok(TrackerResponseMsg::try_from(&buf[..bytes_read])?)
    }

    async fn send_msg(&self, msg: TrackerRequestMsg<'_>) -> TrResult<()> {
        let encoded = msg.encode();
        let sent = self.socket.send(&encoded).await?;

        assert_eq!(encoded.len(), sent);

        Ok(())
    }

    fn check_trans_id(&self, trans_id_1: u32, trans_id_2: u32) -> Result<(), TrErr> {
        if trans_id_1 != trans_id_2 {
            Err(TrErr::TrackerProtocolErr)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
enum TrackerRequestMsg<'h> {
    Connect {
        // Connection ID: 0x41727101980 - u64
        // action: i32
        trans_id: u32,
    },
    Announce {
        conn_id: u64,
        // action: i32
        trans_id: u32,
        info_hash: &'h [u8; 20],
        peer_id: &'h [u8; 20],
        downloaded: u64,
        left: u64,
        uploaded: u64,
        event: ClientState,
        // ip address: default = 0 (use our address)
        // key: u32, random value
        // num_want: u32, default = -1
        port: u16,
    },
}

impl<'h> TrackerRequestMsg<'h> {
    fn new_connect() -> (Self, u32) {
        let trans_id = rand::random();

        (Self::Connect { trans_id }, trans_id)
    }

    fn new_announce(
        conn_id: u64,
        info_hash: &'h [u8; 20],
        peer_id: &'h [u8; 20],
        downloaded: u64,
        left: u64,
        uploaded: u64,
        event: ClientState,
        port: u16,
    ) -> (Self, u32) {
        let trans_id = rand::random();

        let announce = Self::Announce {
            conn_id,
            trans_id,
            info_hash,
            peer_id,
            downloaded,
            left,
            uploaded,
            event,
            port,
        };

        (announce, trans_id)
    }

    fn encode(&self) -> Vec<u8> {
        let put_u16 = |num, buf: &mut Vec<u8>| buf.extend_from_slice(&u16::to_be_bytes(num));
        let put_u32 = |num, buf: &mut Vec<u8>| buf.extend_from_slice(&u32::to_be_bytes(num));
        let put_u64 = |num, buf: &mut Vec<u8>| buf.extend_from_slice(&u64::to_be_bytes(num));

        match *self {
            TrackerRequestMsg::Connect { trans_id } => {
                let mut buf = Vec::with_capacity(16);

                // Magic constant identifying the UDP tracker protocol
                put_u64(0x41727101980, &mut buf);
                put_u32(Action::Connect as u32, &mut buf);
                put_u32(trans_id, &mut buf);

                buf
            }
            TrackerRequestMsg::Announce {
                conn_id,
                trans_id,
                info_hash,
                peer_id,
                downloaded,
                left,
                uploaded,
                event,
                port,
            } => {
                let mut buf = Vec::with_capacity(98);

                put_u64(conn_id, &mut buf);
                put_u32(Action::Announce as u32, &mut buf);
                put_u32(trans_id, &mut buf);

                buf.extend_from_slice(info_hash);
                buf.extend_from_slice(peer_id);

                put_u64(downloaded, &mut buf);
                put_u64(left, &mut buf);
                put_u64(uploaded, &mut buf);
                put_u32(event as u32, &mut buf);
                put_u32(0, &mut buf); // ip
                put_u32(rand::random(), &mut buf); // key
                put_u32(u32::MAX, &mut buf); // numwant
                put_u16(port, &mut buf);

                buf
            }
        }
    }
}

#[derive(Error, Debug)]
enum TrackerMsgEncodeErr {
    #[error("IO error: '{0}'")]
    Io(#[from] std::io::Error),
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
enum TrackerResponseMsg {
    Connect(ConnectResponseMsg),
    Announce(AnnounceResponseMsg),
    Error(ErrorResponseMsg),
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
struct ConnectResponseMsg {
    // action id: u32
    trans_id: u32,
    conn_id: u64,
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
struct AnnounceResponseMsg {
    // ation id: u32
    trans_id: u32,
    interval: u32,
    leechers: u32,
    seeders: u32,
    peers: Vec<SocketAddrV4>,
}

#[derive(Debug, PartialEq)]
struct ErrorResponseMsg {
    // ation id: u32
    trans_id: u32,
    // Seems that this is not a null-terminated string
    error: String,
}

impl TrackerResponseMsg {
    fn expect_conncet(self) -> Result<ConnectResponseMsg, TrErr> {
        match self {
            TrackerResponseMsg::Connect(c) => Ok(c),
            m => {
                tracing::debug!("Unexpected message from UDP tracked: '{:?}'", m);
                Err(TrErr::TrackerProtocolErr)
            }
        }
    }

    fn expect_announce(self) -> Result<AnnounceResponseMsg, TrErr> {
        match self {
            TrackerResponseMsg::Announce(a) => Ok(a),
            m => {
                tracing::debug!("Unexpected message from UDP tracked: '{:?}'", m);
                Err(TrErr::TrackerProtocolErr)
            }
        }
    }
}

impl TryFrom<&[u8]> for TrackerResponseMsg {
    type Error = TrackerMsgDecodeErr;

    fn try_from(mut src: &[u8]) -> Result<Self, TrackerMsgDecodeErr> {
        const MIN_PACKET_LEN: usize = 4 + 4;
        const MIN_CONNECT_LEN: usize = 4 + 4 + 8;
        const MIN_ANNOUNCE_LEN: usize = 4 + 4 + 4 + 4 + 4;
        const SOCKET_ADDR_LEN: usize = 6;

        // src size shrinks with every get_x() !
        let packet_len = src.len();

        if packet_len < MIN_PACKET_LEN {
            return Err(TrackerMsgDecodeErr::MalformedPacket);
        }

        let action = Action::try_from(src.get_u32())?;

        match action {
            Action::Connect => {
                if packet_len < MIN_CONNECT_LEN {
                    return Err(TrackerMsgDecodeErr::MalformedPacket);
                }

                let trans_id = src.get_u32();
                let conn_id = src.get_u64();

                return Ok(TrackerResponseMsg::Connect(ConnectResponseMsg {
                    trans_id,
                    conn_id,
                }));
            }
            Action::Announce => {
                if packet_len < MIN_ANNOUNCE_LEN {
                    return Err(TrackerMsgDecodeErr::MalformedPacket);
                }

                let trans_id = src.get_u32();
                let interval = src.get_u32();
                let leechers = src.get_u32();
                let seeders = src.get_u32();

                let peers: Vec<SocketAddrV4> = src
                    .chunks_exact(SOCKET_ADDR_LEN)
                    .map(|c| {
                        SocketAddrV4::new(
                            Ipv4Addr::new(c[0], c[1], c[2], c[3]),
                            u16::from_be_bytes([c[4], c[5]]),
                        )
                    })
                    .collect();

                return Ok(TrackerResponseMsg::Announce(AnnounceResponseMsg {
                    trans_id,
                    interval,
                    leechers,
                    seeders,
                    peers,
                }));
            }
            Action::Scrape => unimplemented!(),
            Action::Error => {
                // We know that the packet has enough data to hold the error message
                let trans_id = src.get_u32();
                let error = String::from_utf8_lossy(src).to_string();

                Ok(TrackerResponseMsg::Error(ErrorResponseMsg {
                    trans_id,
                    error,
                }))
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum TrackerMsgDecodeErr {
    #[error("IO error: '{0}'")]
    Io(#[from] std::io::Error),
    #[error("Received a malformed packet")]
    MalformedPacket,
}

#[derive(PartialEq)]
#[repr(u32)]
enum Action {
    Connect = 0,
    Announce = 1,
    Scrape = 2,
    Error = 3,
}

impl TryFrom<u32> for Action {
    type Error = TrackerMsgDecodeErr;

    fn try_from(value: u32) -> Result<Self, TrackerMsgDecodeErr> {
        match value {
            0 => Ok(Self::Connect),
            1 => Ok(Self::Announce),
            2 => Ok(Self::Scrape),
            3 => Ok(Self::Error),
            _ => Err(TrackerMsgDecodeErr::MalformedPacket),
        }
    }
}

#[cfg(test)]
mod test_udp_messages {
    use super::*;

    #[test]
    fn test_connect_encode() {
        let (connect_msg, trans_id) = TrackerRequestMsg::new_connect();
        let res_buf = connect_msg.encode();

        let mut expected_buf = vec![
            0, 0, 4, 23, 39, 16, 25, 128, // magic
            0, 0, 0, 0, // action
        ];
        expected_buf.extend_from_slice(&u32::to_be_bytes(trans_id));

        assert_eq!(res_buf, expected_buf);
    }

    #[test]
    fn test_default_announce_encode() {
        let info_hash = [
            0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        ];
        let peer_id = [
            20u8, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
        ];

        let (announce_msg, trans_id) = TrackerRequestMsg::new_announce(
            0xAB_00_CD_BF,
            &info_hash,
            &peer_id,
            0xAA_BB_CC_DD_00_00_00_00,
            0x00_00_EE_FF_00_00_11_22,
            0x00_00_00_00_99_88_77_66,
            ClientState::Started,
            0xAB_CD,
        );
        let buf = announce_msg.encode();

        assert_eq!(buf.len(), 98);
        assert_eq!(&buf[..8], [0, 0, 0, 0, 0xAB, 0, 0xCD, 0xBF]);
        assert_eq!(&buf[8..12], [0, 0, 0, 1]); // announce request = 1
        assert_eq!(&buf[12..16], u32::to_be_bytes(trans_id));
        assert_eq!(&buf[16..36], info_hash);
        assert_eq!(&buf[36..56], peer_id);
        assert_eq!(&buf[56..64], [0xAA, 0xBB, 0xCC, 0xDD, 0, 0, 0, 0]);
        assert_eq!(&buf[64..72], [0, 0, 0xEE, 0xFF, 0, 0, 0x11, 0x22]);
        assert_eq!(&buf[72..80], [0, 0, 0, 0, 0x99, 0x88, 0x77, 0x66]);
        assert_eq!(&buf[80..84], [0, 0, 0, 2]); // ClientState::Started = 2
        assert_eq!(&buf[84..88], [0, 0, 0, 0]); // default IP is 0
                                                // key is random
        assert_eq!(&buf[92..96], [0xFF, 0xFF, 0xFF, 0xFF]); // default numwant = -1
        assert_eq!(&buf[96..98], [0xAB, 0xCD]);
    }

    #[test]
    fn test_connect_decode() {
        let buf = vec![
            0, 0, 0, 0, // action
            0x11, 0x22, 0x33, 0x44, // trans_id
            0x55, 0x66, 0x77, 0x88, 0, 0, 0, 0, // conn_id
        ];

        let got_msg = TrackerResponseMsg::try_from(buf.as_slice()).unwrap();

        let expected_msg = TrackerResponseMsg::Connect(ConnectResponseMsg {
            trans_id: 0x11_22_33_44,
            conn_id: 0x55_66_77_88_00_00_00_00,
        });

        assert_eq!(got_msg, expected_msg);
    }

    #[test]
    fn test_announce_decode() {
        let mut buf = vec![
            0, 0, 0, 1, // action
            0x11, 0x22, 0x33, 0x44, // trans_id
            0x55, 0x66, 0x77, 0x88, // interval_id
            0x99, 0xAA, 0xBB, 0xCC, // leechers_id
            0xDD, 0xEE, 0xFF, 0x00, // seeders_id
                  // no peers for now
        ];

        let got_msg = TrackerResponseMsg::try_from(buf.as_slice()).unwrap();

        let expected_msg = TrackerResponseMsg::Announce(AnnounceResponseMsg {
            trans_id: 0x11_22_33_44,
            interval: 0x55_66_77_88,
            leechers: 0x99_AA_BB_CC,
            seeders: 0xDD_EE_FF_00,
            peers: Vec::new(),
        });

        assert_eq!(got_msg, expected_msg);

        // 2 peers, no extra bytes
        buf.extend_from_slice(&[0, 0x11, 0, 0x22, 0x33, 0x44]);
        buf.extend_from_slice(&[0, 0x55, 0, 0x66, 0x77, 0x88]);

        let got_msg = TrackerResponseMsg::try_from(buf.as_slice()).unwrap();

        let expected_msg = TrackerResponseMsg::Announce(AnnounceResponseMsg {
            trans_id: 0x11_22_33_44,
            interval: 0x55_66_77_88,
            leechers: 0x99_AA_BB_CC,
            seeders: 0xDD_EE_FF_00,
            peers: vec![
                SocketAddrV4::new(Ipv4Addr::new(0, 0x11, 0, 0x22), 0x33_44),
                SocketAddrV4::new(Ipv4Addr::new(0, 0x55, 0, 0x66), 0x77_88),
            ],
        });

        assert_eq!(got_msg, expected_msg);

        // 2 peers, potentially extension bytes at the end
        buf.extend_from_slice(&[23, 42]);

        let got_msg = TrackerResponseMsg::try_from(buf.as_slice()).unwrap();

        let expected_msg = TrackerResponseMsg::Announce(AnnounceResponseMsg {
            trans_id: 0x11_22_33_44,
            interval: 0x55_66_77_88,
            leechers: 0x99_AA_BB_CC,
            seeders: 0xDD_EE_FF_00,
            peers: vec![
                SocketAddrV4::new(Ipv4Addr::new(0, 0x11, 0, 0x22), 0x33_44),
                SocketAddrV4::new(Ipv4Addr::new(0, 0x55, 0, 0x66), 0x77_88),
            ],
        });

        assert_eq!(got_msg, expected_msg);
    }

    #[test]
    fn test_error_decode() {
        let buf = vec![
            0, 0, 0, 3, // action
            0x11, 0x22, 0x33, 0x44, // trans_id
            76, 111, 114, 101, 109, 32, 105, 112, 115, 117, 109, 32, 100, 111, 108, 111, 114, 32,
            115, 105, 116, 32, 97, 109, 101, 116, // message
        ];

        let got_msg = TrackerResponseMsg::try_from(buf.as_slice()).unwrap();

        let expected_msg = TrackerResponseMsg::Error(ErrorResponseMsg {
            trans_id: 0x11_22_33_44,
            error: String::from("Lorem ipsum dolor sit amet"),
        });

        assert_eq!(got_msg, expected_msg);
    }
}
