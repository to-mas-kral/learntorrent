use super::PieceId;

use bytes::BytesMut;

pub struct BitField {
    inner: BytesMut,
}

impl BitField {
    /// Assumes the bitfield has correct length already
    pub fn new(bytes: BytesMut) -> Self {
        Self { inner: bytes }
    }

    pub fn new_empty(num_pieces: u32) -> Self {
        let len = (num_pieces as f32 / 8f32).ceil() as usize;
        let inner = vec![0; len];
        let inner = BytesMut::from(&inner[..]);
        Self { inner }
    }

    /// Is this piece available ?
    pub fn has_piece(&self, id: PieceId) -> bool {
        let byte = self
            .inner
            .get((id / 8) as usize)
            // This shouldn't happen
            .expect("Piece index out of bounds");

        let mask = Self::byte_mask(id);
        (*byte & mask) != 0
    }

    /// Sets the piece of 'id' to available
    pub fn set_piece(&mut self, id: PieceId) {
        let byte = self
            .inner
            .get_mut((id / 8) as usize)
            // This shouldn't happen
            .expect("Piece index out of bounds");

        let mask = Self::byte_mask(id);
        *byte |= mask;
    }

    /// Returns the bitmask corresponding to the piece id
    fn byte_mask(id: PieceId) -> u8 {
        128u8 >> (id % 8)
    }
}

#[cfg(test)]
mod test_super {
    use super::*;

    #[test]
    fn test_bitfield() {
        let bf = vec![0b0000_1000, 0b1001_0001];
        let bf = BytesMut::from(&bf[..]);
        let mut bf = BitField::new(bf);

        assert!(!bf.has_piece(0));
        assert!(!bf.has_piece(3));
        assert!(!bf.has_piece(7));
        assert!(bf.has_piece(4));

        bf.set_piece(0);
        bf.set_piece(3);
        bf.set_piece(7);

        assert!(bf.has_piece(0));
        assert!(bf.has_piece(3));
        assert!(bf.has_piece(7));
        assert!(bf.has_piece(4));

        assert!(bf.has_piece(8));
        assert!(bf.has_piece(11));
        assert!(bf.has_piece(15));

        bf.set_piece(9);
        bf.set_piece(13);

        assert!(bf.has_piece(8));
        assert!(bf.has_piece(9));
        assert!(bf.has_piece(11));
        assert!(bf.has_piece(13));
        assert!(bf.has_piece(15));
    }
}
