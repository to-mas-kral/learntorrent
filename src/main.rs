use tokio::fs;

use crate::{bencoding::bevalue::BeValue, tracker::ClientState};

mod bencoding;
mod metainfo;
mod tracker;

#[tokio::main]
async fn main() {
    let file_contents = fs::read("alpine-standard-3.14.3-x86_64.iso.torrent")
        .await
        .unwrap();
    let contents = BeValue::from_bytes(&file_contents).unwrap();

    let metainfo = metainfo::MetaInfo::from_src_be(&file_contents, contents).unwrap();
    dbg!(&metainfo);

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
    dbg!(response);
}
