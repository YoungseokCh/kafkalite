pub(super) fn partition_for_key(key: &str, partition_count: i32) -> i32 {
    debug_assert!(partition_count > 0);
    (to_positive(murmur2(key.as_bytes())) % partition_count as u32) as i32
}

fn murmur2(data: &[u8]) -> u32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    let mut hash = SEED ^ data.len() as u32;
    let mut offset = 0usize;
    while offset + 4 <= data.len() {
        let mut chunk = u32::from(data[offset]);
        chunk |= u32::from(data[offset + 1]) << 8;
        chunk |= u32::from(data[offset + 2]) << 16;
        chunk |= u32::from(data[offset + 3]) << 24;
        chunk = chunk.wrapping_mul(M);
        chunk ^= chunk >> R;
        chunk = chunk.wrapping_mul(M);
        hash = hash.wrapping_mul(M);
        hash ^= chunk;
        offset += 4;
    }

    match data.len() - offset {
        3 => {
            hash ^= u32::from(data[offset + 2]) << 16;
            hash ^= u32::from(data[offset + 1]) << 8;
            hash ^= u32::from(data[offset]);
            hash = hash.wrapping_mul(M);
        }
        2 => {
            hash ^= u32::from(data[offset + 1]) << 8;
            hash ^= u32::from(data[offset]);
            hash = hash.wrapping_mul(M);
        }
        1 => {
            hash ^= u32::from(data[offset]);
            hash = hash.wrapping_mul(M);
        }
        _ => {}
    }

    hash ^= hash >> 13;
    hash = hash.wrapping_mul(M);
    hash ^ (hash >> 15)
}

fn to_positive(value: u32) -> u32 {
    value & 0x7fff_ffff
}

#[cfg(test)]
mod tests {
    use super::partition_for_key;

    #[test]
    fn kafka_murmur2_partition_vectors_match_expected_values() {
        assert_eq!(partition_for_key("group-multi-0", 50), 26);
        assert_eq!(partition_for_key("group-multi-1", 50), 5);
        assert_eq!(partition_for_key("txn-alpha", 50), 44);
        assert_eq!(partition_for_key("txn-beta", 50), 9);
        assert_eq!(partition_for_key("transactional-id-1", 50), 19);
    }
}
