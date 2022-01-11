use super::{bitfield::BitField, PieceId};

/// Simple mechanism for sequential order piece picking
#[derive(Debug)]
pub struct PieceList {
    /// Blocks are inclusive ranges (PieceId..=PieceId)
    blocks: Vec<(PieceId, PieceId)>,
}

impl PieceList {
    pub fn new_full(num_pieces: u32) -> Self {
        // Num pieces starts from 0
        let mut blocks = vec![(0, num_pieces - 1)];
        blocks.reserve(32);

        Self { blocks }
    }

    pub fn new_empty() -> Self {
        Self {
            blocks: Vec::with_capacity(32),
        }
    }

    pub fn next(&mut self, available_pieces: &BitField) -> Option<PieceId> {
        // Find available piece
        let (block_index, piece) = self.blocks.iter().enumerate().find_map(|(i, block)| {
            for p in block.0..=block.1 {
                if available_pieces.has_piece(p) {
                    return Some((i, p));
                }
            }

            None
        })?;

        // Resize or split the block if needed
        self.split_block_at(block_index, piece);

        Some(piece)
    }

    fn split_block_at(&mut self, block_index: usize, piece: u32) {
        let (mut start, mut end) = self.blocks[block_index];
        if piece == start {
            if start == end {
                self.blocks.remove(block_index);
            } else {
                start += 1;
                self.blocks[block_index] = (start, end);
            }
        } else if piece == end {
            // start == end case is already handled
            end -= 1;
            self.blocks[block_index] = (start, end);
        } else {
            // Split the block into 2
            self.blocks[block_index] = (piece + 1, end);
            self.blocks.insert(block_index, (start, piece - 1));
        }
    }

    pub fn insert(&mut self, pid: PieceId) {
        let len = self.blocks.len();

        for i in 0..=len {
            if i == len {
                self.blocks.push((pid, pid));
                self.compact(i);
                break;
            }

            let (start, end) = self.blocks[i];
            if (start..=end).contains(&pid) {
                return;
            }

            if pid < start {
                self.blocks.insert(i, (pid, pid));
                self.compact(i);
                break;
            }
        }
    }

    // i is the index of the newly-isnerted block
    fn compact(&mut self, i: usize) {
        let len = self.blocks.len();

        if len == 1 {
            return;
        }

        // Length is at least 2
        if i == 0 {
            // i is the start block
            let (head_start, head_end) = self.blocks[i];
            let (tail_start, tail_end) = self.blocks[i + 1];

            if head_end == tail_start - 1 {
                self.blocks[i] = (head_start, tail_end);
                self.blocks.remove(i + 1);
            }
        } else if i == len - 1 {
            // is is the end block
            let (head_start, head_end) = self.blocks[i - 1];
            let (tail_start, tail_end) = self.blocks[i];

            if head_end == tail_start - 1 {
                self.blocks[i - 1] = (head_start, tail_end);
                self.blocks.remove(i);
            }
        } else {
            // i is a middle block
            let (head_start, head_end) = self.blocks[i - 1];
            let (middle_start, middle_end) = self.blocks[i];
            let (tail_start, tail_end) = self.blocks[i + 1];

            let head_middle_continuous = head_end == middle_start - 1;
            let middle_tail_continuous = middle_end == tail_start - 1;

            match (head_middle_continuous, middle_tail_continuous) {
                (true, true) => {
                    if head_middle_continuous && middle_tail_continuous {
                        // Join all 3 blocks together
                        self.blocks[i - 1] = (head_start, tail_end);
                        self.blocks.remove(i + 1);
                        self.blocks.remove(i);
                    }
                }
                (true, false) => {
                    // Join previous and current blocks
                    self.blocks[i - 1] = (head_start, middle_end);
                    self.blocks.remove(i);
                }
                (false, true) => {
                    // Join current and next blocks
                    self.blocks[i] = (middle_start, tail_end);
                    self.blocks.remove(i + 1);
                }
                _ => (),
            }
        }
    }

    /// Returns Some(block_index) if a specific piece is present in this list
    pub fn contains(&self, pid: PieceId) -> Option<usize> {
        for (index, (start, end)) in self.blocks.iter().enumerate() {
            if (*start..=*end).contains(&pid) {
                return Some(index);
            }
        }

        None
    }

    pub fn all_finished(&self, piece_count: u32) -> bool {
        self.blocks.len() == 1 && self.blocks[0] == (0, piece_count - 1)
    }
}

#[cfg(test)]
mod test_super {
    use super::*;

    #[test]
    fn test_piece_list_queue() {
        let mut ap = BitField::new_empty(33);

        let mut pq = PieceList::new_full(33);
        assert_eq!(pq.blocks, &[(0, 32)]);

        ap.set_piece(16);
        ap.set_piece(4);
        assert_eq!(pq.next(&ap).unwrap(), 4);
        assert_eq!(pq.next(&ap).unwrap(), 16);

        assert_eq!(pq.blocks, &[(0, 3), (5, 15), (17, 32)]);
    }

    #[test]
    fn test_piece_list() {
        let mut pl = PieceList::new_empty();

        pl.insert(10);
        pl.insert(10);
        assert_eq!(pl.blocks, [(10, 10)]);

        pl.insert(9);
        assert_eq!(pl.blocks, [(9, 10)]);
        pl.insert(11);
        assert_eq!(pl.blocks, [(9, 11)]);

        pl.insert(15);
        assert_eq!(pl.blocks, [(9, 11), (15, 15)]);

        pl.insert(16);
        assert_eq!(pl.blocks, [(9, 11), (15, 16)]);

        pl.insert(12);
        pl.insert(14);
        pl.insert(13);
        assert_eq!(pl.blocks, [(9, 16)]);

        pl.insert(19);
        assert_eq!(pl.blocks, [(9, 16), (19, 19)]);

        pl.insert(4);
        pl.insert(5);
        pl.insert(24);
        pl.insert(32);
        assert_eq!(pl.blocks, [(4, 5), (9, 16), (19, 19), (24, 24), (32, 32)]);

        // Insert before blocks
        pl.insert(31);
        pl.insert(23);
        pl.insert(8);
        pl.insert(3);
        assert_eq!(pl.blocks, [(3, 5), (8, 16), (19, 19), (23, 24), (31, 32)]);

        // Insert after blocks
        pl.insert(6);
        pl.insert(17);
        pl.insert(20);
        pl.insert(25);
        pl.insert(33);
        assert_eq!(pl.blocks, [(3, 6), (8, 17), (19, 20), (23, 25), (31, 33)]);

        // Contains
        pl.contains(3).unwrap();
        pl.contains(8).unwrap();
        pl.contains(17).unwrap();
        pl.contains(31).unwrap();
        pl.contains(33).unwrap();

        // Does not contain
        assert!(pl.contains(2).is_none());
        assert!(pl.contains(7).is_none());
        assert!(pl.contains(18).is_none());
        assert!(pl.contains(21).is_none());
        assert!(pl.contains(22).is_none());
        assert!(pl.contains(26).is_none());
        assert!(pl.contains(30).is_none());
        assert!(pl.contains(34).is_none());

        // Compaction
        pl.insert(7);
        pl.insert(18);
        pl.insert(22);
        pl.insert(21);
        pl.insert(30);
        pl.insert(26);
        pl.insert(29);
        pl.insert(27);
        pl.insert(28);
        assert_eq!(pl.blocks, [(3, 33)]);
    }

    #[test]
    fn test_piece_list_compact() {
        let mut pl = PieceList::new_empty();
        pl.insert(0);
        pl.insert(1);
        pl.insert(3);
        pl.insert(4);
        pl.insert(5);
        pl.insert(7);
        pl.insert(8);
        pl.insert(10);
        assert_eq!(pl.blocks, [(0, 1), (3, 5), (7, 8), (10, 10)]);

        pl.insert(6);
        assert_eq!(pl.blocks, [(0, 1), (3, 8), (10, 10)]);
    }
}
