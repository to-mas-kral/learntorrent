use std::fs::read;

use crate::bencoding::BeValue;

mod bencoding;

fn main() {
    let file_contents = read("Karetka.scad.torrent").unwrap();
    let contents = BeValue::from_bytes(&file_contents).unwrap();
    println!("{:?}", contents);
}
