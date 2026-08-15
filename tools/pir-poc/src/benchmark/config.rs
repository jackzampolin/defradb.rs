use super::Profile;

pub(super) const TARGET_SERVER_COUNTS: [usize; 2] = [2, 3];
pub(super) const LOAD_BUCKET_COUNT: usize = 1 << 20;
pub(super) const LOAD_ROW_SIZE: usize = 64;

pub(super) fn dimensions(profile: Profile) -> Vec<(usize, usize)> {
    match profile {
        Profile::Quick => vec![
            (1 << 14, 64),
            (1 << 14, 256),
            (1 << 18, 64),
            (1 << 18, 256),
            (1 << 20, 64),
            (1 << 22, 64),
        ],
        Profile::Full => vec![
            (1 << 14, 64),
            (1 << 14, 256),
            (1 << 18, 64),
            (1 << 18, 256),
            (1 << 20, 64),
            (1 << 20, 256),
            (1 << 20, 1024),
            (1 << 22, 64),
        ],
    }
}

pub(super) fn batch_sizes(profile: Profile) -> Vec<usize> {
    match profile {
        Profile::Quick => vec![1, 8, 32],
        Profile::Full => vec![1, 8, 32, 128],
    }
}

pub(super) fn sample_count(profile: Profile, snapshot_bytes: usize) -> usize {
    match profile {
        Profile::Quick if snapshot_bytes >= 64 * 1024 * 1024 => 3,
        Profile::Quick => 7,
        Profile::Full => 11,
    }
}
