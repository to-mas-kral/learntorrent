![Rust build and tests](https://github.com/TomasKralCZ/learntorrent/actions/workflows/rust.yml/badge.svg)

This is a simple BitTorrent client using Tokio. The client communicates with other clients
using TCP. Currently only leeching is implemented. The pieces are picked (mostly) sequentially and an [endgame](https://wiki.theory.org/BitTorrentSpecification#End_Game) mode is implemented.

File I/O is performed using io_uring. Each piece is written using one `writev` opcode.
There is more room for improvement in this area, but a basic io_uring setup is fast enough.

# Architecture
![Architecture](resources/diagram.svg)

# Questions
- [ ] Tracing on panic ?

# TODO
- [ ] Periodically contacting the trackers to get new peers
- [ ] Rarest-first piece picking algorithm
- [ ] [FastPeers](https://wiki.theory.org/BitTorrentSpecification#Fast_Peers_Extensions) extension
- [ ] [DHT](https://wiki.theory.org/BitTorrentSpecification#Distributed_Hash_Table)

# Sources
- Unofficial specification: https://wiki.theory.org/BitTorrentSpecification <br/>
- UDP tracker protocol specs: https://github.com/steeve/libtorrent/blob/master/docs/udp_tracker_protocol.rst#request-string <br/>

- Guide - https://www.seanjoflynn.com/research/bittorrent.html <br/>
- Guide: https://blog.jse.li/posts/torrent/ <br/>